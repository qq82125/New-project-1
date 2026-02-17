FROM python:3.11-slim

WORKDIR /app

# Minimal OS deps: tzdata for ZoneInfo, curl for SMTP send script healthchecks.
RUN apt-get update \
  && apt-get install -y --no-install-recommends tzdata curl ca-certificates \
  && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

# Default ports are controlled via env in compose.
EXPOSE 8789

