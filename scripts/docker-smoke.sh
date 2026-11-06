#!/usr/bin/env bash
# Смоук-тест Docker-образа: мигратор применяет миграции, сервис запускается,
# отвечает на health-check и корректно останавливается по SIGTERM (docker stop).
#
# Использование: scripts/docker-smoke.sh IMAGE
# Серверы задаются переменными DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME,
# KAFKA_PRODUCER_BROKERS и REDIS_CACHE_ADDR. Контейнеры работают в сети хоста
# (--network host): так в Linux и WSL2 контейнер видит серверы на localhost,
# а порт сервиса доступен с хоста.
set -euo pipefail

IMAGE="${1:?usage: $0 IMAGE}"
NAME="place-svc-smoke"
PORT="${GRPCSERVER_PORT:-50052}"

db_env="$(mktemp)"
svc_env="$(mktemp)"
cleanup() {
	docker rm -f "$NAME" >/dev/null 2>&1 || true
	rm -f "$db_env" "$svc_env"
}
trap cleanup EXIT

# Значения без кавычек: docker передаёт их в контейнер как есть.
# Мигратору нужны только настройки БД, сервису - и все остальные.
cat >"$db_env" <<EOF
DB_USER=${DB_USER:-postgres}
DB_PASSWORD=${DB_PASSWORD:-postgres}
DB_NAME=${DB_NAME:-place_svc_test}
DB_HOST=${DB_HOST:-localhost}
DB_PORT=${DB_PORT:-5432}
EOF
cat "$db_env" - >"$svc_env" <<EOF
ENV=prod
GRPCSERVER_PORT=${PORT}
GRACEFUL_TIMEOUT=10s
KAFKA_PRODUCER_BROKERS=${KAFKA_PRODUCER_BROKERS:-localhost:9092}
REDIS_CACHE_ADDR=${REDIS_CACHE_ADDR:-localhost:6379}
REDIS_CACHE_TTL=45s
EOF

fail() {
	echo "❌  $1"
	docker logs "$NAME" 2>&1 || true
	exit 1
}

echo "⚙️   Миграции..."
docker run --rm --network host --env-file "$db_env" --entrypoint /app/migrator "$IMAGE"

echo "🚀  Запуск сервиса..."
docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" --network host --env-file "$svc_env" "$IMAGE" >/dev/null

echo "🩺  Ожидание health-check..."
healthy=""
for _ in $(seq 1 30); do
	if docker exec "$NAME" /bin/grpc_health_probe -addr=":${PORT}" >/dev/null 2>&1; then
		healthy=1
		break
	fi
	if [ "$(docker inspect -f '{{.State.Running}}' "$NAME")" != "true" ]; then
		fail "Контейнер завершился до health-check"
	fi
	sleep 1
done
[ -n "$healthy" ] || fail "Сервис не ответил на health-check за 30 секунд"

echo "🛑  Остановка (SIGTERM)..."
docker stop -t 15 "$NAME" >/dev/null
exit_code="$(docker inspect -f '{{.State.ExitCode}}' "$NAME")"
[ "$exit_code" = "0" ] || fail "Код выхода $exit_code, ожидался 0"
docker logs "$NAME" 2>&1 | grep -q "app has shut down gracefully" ||
	fail "В логах нет записи о корректной остановке"

echo "✅  Образ $IMAGE прошёл смоук-тест"
