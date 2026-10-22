# Асинхронный процессинг платежей

Тестовое задание: FastAPI + Pydantic v2, async SQLAlchemy, PostgreSQL, RabbitMQ / FastStream, Alembic. Внешний платежный шлюз эмулируется, реальных списаний нет.

## Запуск

Нужен Docker с Compose v2:

```bash
cp .env.example .env
# Для внешнего окружения измените API_KEY в .env.
docker compose up -d --build
curl http://localhost:8000/health
```

Миграции выполняет отдельный одноразовый контейнер `migrate`. API и consumer запускаются после успешного завершения миграций. Swagger: http://localhost:8000/docs. RabbitMQ Management: http://localhost:15673, локальные demo-данные входа `payments / payments`.

Порты опубликованы только на loopback. PostgreSQL и RabbitMQ хранят данные в именованных томах. `docker compose down` останавливает окружение, сохраняя данные.

## API

Оба endpoint требуют статический заголовок `X-API-Key`. Создание дополнительно требует `Idempotency-Key` (1-255 символов).

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: local-demo-change-me' \
  -H 'Idempotency-Key: order-42' \
  -H 'Content-Type: application/json' \
  -d '{"amount":"150.00","currency":"RUB","description":"Order 42","metadata":{"order_id":42},"webhook_url":"https://merchant.example/webhook"}'
```

Ответ `202 Accepted`:

```json
{"payment_id":"f68d68cd-0fc8-4899-89b3-3e87fdb841a7","status":"pending","created_at":"2026-10-22T08:00:00Z"}
```

```bash
curl http://localhost:8000/api/v1/payments/PAYMENT_ID \
  -H 'X-API-Key: local-demo-change-me'
```

GET возвращает сумму, валюту, описание, метаданные, статус, даты, webhook URL и независимое состояние доставки `delivery_state`: `pending / delivered / dead`.

- Повтор того же ключа и тела возвращает тот же платеж, без нового события. `1`, `1.0` и `1.00` считаются одной суммой.
- Повтор ключа с другим телом: `409 Conflict`.
- Ключ проверяется уникальным ограничением PostgreSQL; конкурентные запросы корректно сериализуются.
- Недопустимые суммы (<= 0, более двух знаков после запятой), валюта или URL: `422`.
- Неверный / отсутствующий API key: `401`. Неизвестный платеж: `404`.
- Денежные суммы хранятся как `NUMERIC(18,2)` и сериализуются строками, без float.

## Обработка и гарантии

1. **Одна транзакция:** создание `payments` и записи `outbox` с маршрутом `payments.new`. API не зависит от доступности RabbitMQ.
2. **Outbox relay:** фоновая задача в процессе consumer выбирает доступные записи через `FOR UPDATE SKIP LOCKED`. Публикует persistent-сообщение с `mandatory=True` и publisher confirm, затем отмечает публикацию в БД. Неуспешная публикация откатывает транзакцию, запись остается для следующей попытки. Несколько relay не забирают одну запись одновременно.
3. **Один обработчик:** FastStream consumer принимает событие, блокирует платеж, эмулирует шлюз за 2-5 секунд и сохраняет конечный статус `succeeded / failed`. Результат стабилен для одного UUID; 90% пространства UUID соответствует успеху, 10% - отказу. Это моделирует идемпотентный шлюз и предотвращает смену результата после перезапуска.
4. **Webhook:** статус фиксируется до HTTP-запроса. Отказ платежного шлюза тоже доставляется клиенту как результат `failed`. Отказ доставки webhook не меняет результат платежа.
5. **Три попытки всего:** при сетевой ошибке или HTTP не-2xx счетчик попыток и следующий event сохраняются одной транзакцией. Повторы планируются outbox через 1 и 2 секунды (экспоненциальный backoff). Счетчик переживает перезапуск. Повтор старого сообщения не расходует новую попытку.
6. **DLQ:** после третьего неуспешного webhook состояние становится `dead`, финальное событие через outbox попадает в durable `payments.dlq` (exchange `payments`, routing key `payments.dead`). Непредвиденные исключения обработчика приводят к NACK; quorum-очередь дополнительно ограничивает транспортные redelivery через `x-delivery-limit=3` и направляет poison message в ту же DLQ. Это отдельная защита от технических сбоев, не счетчик HTTP-попыток.

Publisher confirm и commit БД не являются общей транзакцией: возможен повтор публикации при падении между ними. Поэтому семантика **at-least-once**, а обработка защищена блокировками, состоянием платежа и номером попытки. `message_id` равен UUID outbox-записи.

Webhook имеет стабильный `event_id = payment:<UUID>:result`, также передаваемый заголовком `Idempotency-Key`. При падении после успешного HTTP, но до commit возможна повторная доставка. Получатель должен дедуплицировать по `event_id`. Exactly-once внешнего HTTP без участия получателя не обещается.

HTTP timeout: 10 секунд. Redirect отключен. Ошибки broker relay логируются и повторяются с интервалом 0,5 секунды; бизнес-повторы webhook используют свой backoff. Завершение consumer дожидается остановки relay.

## Сквозная проверка через Docker

Есть локальный получатель webhook, не требующий внешних сервисов:

```bash
docker compose -f compose.yaml -f compose.demo.yaml up -d --build
# При первом запуске дождитесь healthy API:
curl http://localhost:8000/health
uv sync --frozen
uv run python examples/smoke.py
```

Скрипт создает три платежа и проверяет: успешную доставку с первого раза, успех после двух HTTP 503, окончательный отказ после трех попыток и наличие сообщения в реальной RabbitMQ DLQ. Проверяет повторный POST с тем же ключом и стабильность event_id. Получатель `webhook` доступен из Docker-сети; адрес `localhost` внутри контейнера означает сам контейнер, а не хост.

## Автотесты

Нужны Python 3.12+, `uv` и PostgreSQL из Compose. Тесты удаляют свои таблицы данных, поэтому используйте отдельную БД `payments_test`, никогда рабочую БД:

```bash
docker compose up -d postgres rabbitmq
docker compose exec postgres createdb -U payments payments_test
uv sync --frozen
export API_KEY=local-demo-change-me
export DATABASE_URL=postgresql+asyncpg://payments:payments@localhost:55432/payments_test
uv run alembic upgrade head
uv run alembic check
uv run pytest -q
uv run ruff check app migrations tests examples
```

Покрыты auth/валидация, конкурентная идемпотентность, конфликт тела, создание outbox в транзакции, оба исхода шлюза, повторная доставка события, HTTP retry / backoff / DLQ и откат маркера outbox при сбое publisher. Тесты используют настоящую PostgreSQL; HTTP и broker в компонентных тестах подменяются. Отдельный smoke-прогон использует настоящие RabbitMQ и HTTP. GitHub Actions выполняет компонентные тесты и проверку миграций.

## Границы тестового решения

- API key общий для сервиса; пользовательская / tenant-изоляция не реализована.
- Webhook URL задает доверенный клиент с API key. Для публичного multi-tenant окружения нужны allowlist получателей, защита от SSRF / DNS rebinding и сетевые egress-ограничения. Приватные адреса оставлены доступными для локального Docker demo.
- Нет цифровой подписи webhook, мониторинга с алертами и административного API для повторного запуска DLQ. В промышленном решении потребуются HMAC-подпись, secret management, ротация API key, ограничение частоты запросов и регламент replay.
- Финансовые действия эмулируются. При замене шлюза реальный provider должен принимать стабильный idempotency key и позволять восстановить результат после таймаута.
- Старые опубликованные записи outbox в рамках тестового сохраняются; для долгой эксплуатации нужна политика очистки / архивации.

## Документация используемых механизмов

- [FastStream RabbitBroker](https://faststream.ag2.ai/latest/api/faststream/rabbit/RabbitBroker/)
- [RabbitMQ publisher confirms и acknowledgements](https://www.rabbitmq.com/docs/confirms)
- [PostgreSQL locking clauses](https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE)
