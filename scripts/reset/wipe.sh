#!/usr/bin/env bash
# Полный сброс тестового стенда: контейнеры, сети и volumes ТОЛЬКО этого проекта
# (compose project streaming-polling-outbox). Данные других Docker-проектов
# не затрагиваются (requirements.md раздел 11).
set -euo pipefail
cd "$(dirname "$0")/../.."

echo "==> Остановка и удаление контейнеров, сетей и volumes стенда..."
# ВАЖНО: обе --profile обязаны быть в down. Без них compose останавливает только
# сервисы БЕЗ профиля — cdc-polling/cdc-streaming (profiles polling/streaming)
# переживают wipe: контейнер остаётся живым, держит сеть проекта («Resource is
# still in use») и после нового up reconnect-ится к НОВОМУ кластеру, занимая
# logical slot (грабли этапа 9).
docker compose --profile polling --profile streaming down -v --remove-orphans --timeout 30
echo "==> Готово. Стенд полностью удалён. Для запуска с нуля: docker compose --profile <arm> up -d --build"
