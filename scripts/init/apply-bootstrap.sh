#!/usr/bin/env bash
# Ручной повторный запуск идемпотентной инициализации БД (роли, БД, publication,
# слот, userlist PgBouncer). Безопасен при повторном вызове.
set -euo pipefail
cd "$(dirname "$0")/../.."

docker compose run --rm init-db
