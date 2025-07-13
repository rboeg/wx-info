FROM python:3.13-slim

WORKDIR /wx-info

RUN pip install --upgrade pip && \
    pip install poetry

COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root

# Copy the project files
COPY ./app ./app
COPY ./tests ./tests
COPY ./scripts ./scripts
COPY .env ./.env

# Set PYTHONPATH so 'app' is always importable
ENV PYTHONPATH=/wx-info

# Entrypoint script to initialize DB and start API
RUN chmod +x ./scripts/entrypoint.sh

CMD ["./scripts/entrypoint.sh"] 