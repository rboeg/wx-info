FROM python:3.13-slim

WORKDIR /app

RUN pip install --upgrade pip && \
    pip install poetry

COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root

COPY app/ ./
COPY .env ./.env

# Entrypoint script to initialize DB and start API
COPY scripts/entrypoint.sh ./entrypoint.sh
RUN chmod +x ./entrypoint.sh

CMD ["./entrypoint.sh"] 