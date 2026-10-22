import hashlib
import json
import secrets
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import engine, session_dependency
from app.models import Outbox, Payment
from app.schemas import CreatePayment, PaymentCreated, PaymentDetails


@asynccontextmanager
async def lifespan(app):
    yield
    await engine.dispose()


app = FastAPI(title="Async Payment Processing", lifespan=lifespan)


def authenticate(x_api_key: Annotated[str | None, Header()] = None):
    if not x_api_key or not secrets.compare_digest(x_api_key.encode(), settings.api_key.encode()):
        raise HTTPException(401, "Invalid API key")


@app.get("/health", include_in_schema=False)
async def health(session: Annotated[AsyncSession, Depends(session_dependency)]):
    await session.execute(text("SELECT 1"))
    return {"status": "ok"}


@app.post(
    "/api/v1/payments",
    status_code=202,
    response_model=PaymentCreated,
    dependencies=[Depends(authenticate)],
)
async def create_payment(
    body: CreatePayment,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
    session: Annotated[AsyncSession, Depends(session_dependency)],
):
    canonical = body.model_dump(mode="json")
    # Equivalent decimal representations (1, 1.0, 1.00) must hash identically.
    canonical["amount"] = format(body.amount, ".2f")
    digest = hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    async with session.begin():
        stmt = (
            insert(Payment)
            .values(
                amount=body.amount,
                currency=body.currency,
                description=body.description,
                metadata_=body.metadata,
                webhook_url=str(body.webhook_url),
                idempotency_key=idempotency_key,
                request_hash=digest,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        payment = (await session.execute(stmt)).scalar_one_or_none()
        if payment is None:
            payment = (
                await session.execute(
                    select(Payment).where(Payment.idempotency_key == idempotency_key)
                )
            ).scalar_one()
            if payment.request_hash != digest:
                raise HTTPException(409, "Idempotency-Key already used with different data")
        else:
            session.add(
                Outbox(topic="payments.new", payload={"payment_id": str(payment.id), "attempt": 1})
            )
    return PaymentCreated(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@app.get(
    "/api/v1/payments/{payment_id}",
    response_model=PaymentDetails,
    dependencies=[Depends(authenticate)],
)
async def get_payment(
    payment_id: UUID, session: Annotated[AsyncSession, Depends(session_dependency)]
):
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(404, "Payment not found")
    return PaymentDetails(
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.metadata_,
        webhook_url=payment.webhook_url,
        status=payment.status,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
        delivery_state=payment.delivery_state,
    )
