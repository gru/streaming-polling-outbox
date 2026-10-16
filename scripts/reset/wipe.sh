#!/usr/bin/env bash
# Полный сброс тестового стенда: контейнеры, сети и volumes ТОЛЬКО этого проекта
# (compose project streaming-polling-outbox). Данные других Docker-проектов
# не затрагиваются (requirements.md раздел 11).
set -euo pipefail
cd "$(dirname "$0")/../.."

echo "==> Остановка и удаление контейнеров, сетей и volumes стенда..."
docker compose down -v --remove-orphans --timeout 30
echo "==> Готово. Стенд полностью удалён. Для запуска с нуля: docker compose up -d --build"
