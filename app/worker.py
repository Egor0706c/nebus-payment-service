import asyncio
import logging
import random
from datetime import UTC, datetime, timedelta
from uuid import UUID

import httpx
from faststream import AckPolicy, FastStream
from pydantic import BaseModel, Field
from sqlalchemy import select

from app.config import settings
from app.db import Session, engine
from app.messaging import broker, exchange, queue, topology
from app.models import Outbox, Payment
from app.outbox import relay

log = logging.getLogger(__name__)
app = FastStream(broker)
stop = asyncio.Event()
relay_task = None


class Event(BaseModel):
    payment_id: UUID
    attempt: int = Field(ge=1, le=3)


async def emulate_gateway(payment_id: UUID):
    await asyncio.sleep(random.uniform(settings.gateway_min_seconds, settings.gateway_max_seconds))
    # Deterministic 90/10 partition: the same payment_id never gets a second outcome
    # after a worker crash. A real provider must support this idempotency key too.
    return "succeeded" if payment_id.int % 10 < 9 else "failed"


async def process(event: Event):
    async with Session.begin() as session:
        payment = await session.scalar(
            select(Payment).where(Payment.id == event.payment_id).with_for_update()
        )
        if payment is None:
            raise ValueError("Unknown payment")
        if payment.status == "pending":
            payment.status = await emulate_gateway(payment.id)
            payment.processed_at = datetime.now(UTC)
    # Commit the gateway result before contacting the client; webhook retries never recharge.
    async with Session.begin() as session:
        payment = await session.scalar(
            select(Payment).where(Payment.id == event.payment_id).with_for_update()
        )
        if payment.delivery_state in {"delivered", "dead"} or event.attempt <= payment.attempts:
            return
        if event.attempt != payment.attempts + 1:
            raise ValueError("Out-of-order retry")
        payload = {
            "event_id": f"payment:{payment.id}:result",
            "payment_id": str(payment.id),
            "status": payment.status,
            "amount": format(payment.amount, ".2f"),
            "currency": payment.currency,
            "processed_at": payment.processed_at.isoformat(),
        }
        try:
            # No redirects or proxy env: do not silently send callbacks to a different host.
            async with httpx.AsyncClient(
                timeout=10, follow_redirects=False, trust_env=False
            ) as client:
                response = await client.post(
                    payment.webhook_url,
                    json=payload,
                    headers={"Idempotency-Key": payload["event_id"]},
                )
                if not 200 <= response.status_code < 300:
                    raise httpx.HTTPStatusError(
                        "Webhook rejected", request=response.request, response=response
                    )
        except httpx.HTTPError:
            payment.attempts = event.attempt
            if event.attempt >= 3:
                payment.delivery_state = "dead"
                session.add(
                    Outbox(
                        topic="payments.dead",
                        payload={
                            **payload,
                            "attempts": event.attempt,
                            "reason": "webhook_delivery_failed",
                        },
                    )
                )
            else:
                delay = settings.retry_base_seconds * 2 ** (event.attempt - 1)
                session.add(
                    Outbox(
                        topic="payments.new",
                        payload={"payment_id": str(payment.id), "attempt": event.attempt + 1},
                        available_at=datetime.now(UTC) + timedelta(seconds=delay),
                    )
                )
            log.warning("Webhook failed for payment %s, attempt %s", payment.id, event.attempt)
        else:
            payment.attempts = event.attempt
            payment.delivery_state = "delivered"
            payment.webhook_sent_at = datetime.now(UTC)


@broker.subscriber(queue, exchange=exchange, ack_policy=AckPolicy.NACK_ON_ERROR)
async def consume(body: dict):
    await process(Event.model_validate(body))


@app.after_startup
async def startup():
    global relay_task
    await topology()
    relay_task = asyncio.create_task(relay(stop))


@app.on_shutdown
async def shutdown():
    stop.set()
    if relay_task:
        await relay_task
    await engine.dispose()
