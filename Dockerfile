# syntax=docker/dockerfile:1

# Версия Go вынесена в аргумент, чтобы её было легко поменять
# без правки остального файла: docker build --build-arg GO_VERSION=1.27 .
ARG GO_VERSION=1.26
# Версия утилиты grpc_health_probe для проверки состояния сервиса
ARG GRPC_HEALTH_PROBE_VERSION=v0.4.38

# ============================================================
# Стадия 1: build — компилируем бинарники
# ============================================================
FROM golang:${GO_VERSION}-alpine AS build

WORKDIR /src

# Откуда Go скачивает модули. По умолчанию официальный прокси,
# но его можно переопределить при сборке:
#   docker build --build-arg GOPROXY=https://goproxy.io,direct .
ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY}

# Статическая сборка без C-зависимостей: такие бинарники
# запустятся в минимальном образе без libc.
ENV CGO_ENABLED=0

# Сначала копируем только файлы зависимостей и скачиваем модули.
# Этот слой пересоберётся, только если изменятся go.mod / go.sum.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Теперь копируем исходники и собираем оба бинарника:
# сам сервис и мигратор, применяющий миграции БД.
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/place-svc ./cmd/svc-starter && \
    go build -trimpath -ldflags="-s -w" -o /out/migrator ./cmd/migrator

# ============================================================
# Стадия health-probe: готовый образ с утилитой grpc_health_probe
# ============================================================
FROM ghcr.io/grpc-ecosystem/grpc-health-probe:${GRPC_HEALTH_PROBE_VERSION} AS health-probe

# ============================================================
# Стадия 2: runtime — минимальный образ с бинарниками и миграциями
# ============================================================
FROM gcr.io/distroless/static:nonroot AS runtime

# Мигратор ищет SQL-файлы в ./migrations относительно рабочей директории.
WORKDIR /app

COPY --from=build /out/place-svc /out/migrator /app/
COPY migrations /app/migrations
COPY --from=health-probe /ko-app/grpc-health-probe /bin/grpc_health_probe

# Запуск от непривилегированного пользователя (UID 65532).
USER nonroot:nonroot

# Документация: порт gRPC-сервера внутри контейнера (задаётся через GRPCSERVER_PORT).
EXPOSE 50052

# По умолчанию запускается сервис. Миграции применяются тем же образом:
#   docker run --rm --env-file .env --entrypoint /app/migrator place-svc
ENTRYPOINT ["/app/place-svc"]