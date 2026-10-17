#!/usr/bin/env bash
# Ручной повторный запуск идемпотентной инициализации БД (роли, БД, publication,
# слот, userlist PgBouncer). Безопасен при повторном вызове.
set -euo pipefail
cd "$(dirname "$0")/../.."

docker compose run --rm --no-deps init-db
# --no-deps: без реконсиляции графа зависимостей — иначе compose пересоздаёт
# pg-контейнеры и роняет живой Patroni-кластер (init-db сам ждёт haproxy в цикле).
