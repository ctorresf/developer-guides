#!/usr/bin/env bash
set -euo pipefail

if ! command -v rabbitmqctl >/dev/null 2>&1; then
  echo "RabbitMQ is not installed. Ensure install-dependencies.sh completed successfully."
  exit 1
fi

if sudo rabbitmqctl status >/dev/null 2>&1; then
  echo "RabbitMQ already running"
else
  echo "Starting RabbitMQ server..."
  sudo rabbitmq-server -detached
  sleep 5
  sudo rabbitmqctl status
  echo "RabbitMQ started"
fi

if command -v gcloud >/dev/null 2>&1; then
  echo "gcloud is installed: $(gcloud --version | head -n 1)"
else
  echo "gcloud CLI is not installed"
fi
