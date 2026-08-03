# Читаем GRPCSERVER_PORT из .env
GRPCSERVER_PORT := $(shell grep -m1 '^GRPCSERVER_PORT=' .env | cut -d'=' -f2- | xargs)
ifeq ($(GRPCSERVER_PORT),)
  GRPCSERVER_PORT := 50051
endif

GRPC_ADDR := localhost:$(GRPCSERVER_PORT)

PROTOS_DIR ?= ../protos
PROTOSET   := $(PROTOS_DIR)/place_service_v1.protoset

PUBLIC_SERVICE_NAME := gateway-svc

USER_FILE := .user_id
USER_ID := $(shell cat $(USER_FILE) 2>/dev/null)

GRPCURL := grpcurl -plaintext -protoset $(PROTOSET)

.PHONY: help protoset run-svc set-user add-place my-places places clean

help:
	@echo "Доступные команды:"
	@echo "  make protoset       - Сгенерировать protoset (запускается в ../protos)"
	@echo "  make run-svc        - Запустить Place Service (go run)"
	@echo "  make set-user       - Сохранить ID пользователя (вспомогательная команда)"
	@echo "  make add-place      - Добавить место в список пользователя"
	@echo "  make my-places      - Список своих мест (по сохранённому ID)"
	@echo "  make places         - Список мест пользователя по ID"
	@echo "  make clean          - Удалить сохранённый ID"

protoset:
	@if [ ! -d $(PROTOS_DIR) ]; then \
		echo "❌  Папка $(PROTOS_DIR) не найдена, клонируйте репозиторий protos согласно README"; \
		exit 1; \
	fi
	@$(MAKE) -C $(PROTOS_DIR) protoset-place
	@echo "✅  Protoset обновлён: $(PROTOSET)"

# Проверка наличия protoset перед вызовами
_check_protoset:
	@test -f $(PROTOSET) || { \
		echo "❌  Protoset-файл не найден, выполните: make protoset"; \
		exit 1; \
	}

run-svc:
	@echo "🚀  Применение миграций и запуск Place Service..."
	@go run ./cmd/migrator/main.go && go run ./cmd/svc-starter/main.go; exit 0

set-user:
	@echo "Определение пользователя, от лица которого будут отправляться запросы"; \
	read -p "ID пользователя: " id; \
	echo "$$id" > $(USER_FILE); \
	chmod 600 $(USER_FILE); \
	echo "🆔  ID пользователя сохранён в $(USER_FILE)"; 

add-place: _check_protoset
	@test -f $(USER_FILE) || { echo "❌  Сначала выполните make set-user"; exit 1; }
	@echo "Запрос на добавление нового места"; \
	read -p "Название места: " name; \
	read -p "Описание места: " info; echo; \
	echo "📬  Ответ сервера:"; \
	resp=$$($(GRPCURL) \
	  -H 'x-service-name: $(PUBLIC_SERVICE_NAME)' \
	  -H 'x-user-id: $(USER_ID)' \
	  -d "{\"name\":\"$$name\",\"info\":\"$$info\"}" \
	  $(GRPC_ADDR) place.v1.PlaceService/AddPlace); \
	if echo "$$resp" | grep -qE '^[\[\{]'; then \
	    echo "$$resp" | jq .; \
	    echo "✅  Место добавлено"; \
	else \
	    echo "$$resp"; \
	    echo "❌  Что-то пошло не так..."; \
	fi

my-places: _check_protoset
	@test -f $(USER_FILE) || { echo "❌  Сначала выполните make set-user"; exit 1; }
	@echo "Запрос на получение списка своих мест"; \
	echo "📬  Ответ сервера:"; \
	resp=$$($(GRPCURL) \
	  -H 'x-service-name: $(PUBLIC_SERVICE_NAME)' \
	  -H 'x-user-id: $(USER_ID)' \
	  -d "{\"user_id\":\"$(USER_ID)\"}" \
	  $(GRPC_ADDR) place.v1.PlaceService/GetUserPlaces); \
	if echo "$$resp" | grep -qE '^[\[\{]'; then \
	    echo "$$resp" | jq .; \
	    echo "✅  Места получены"; \
	else \
	    echo "$$resp"; \
	    echo "❌  Что-то пошло не так..."; \
	fi

places: _check_protoset
	@test -f $(USER_FILE) || { echo "❌  Сначала выполните make set-user"; exit 1; }
	@echo "Запрос на получение списка мест пользователя"; \
	read -p "ID пользователя: " id; \
	echo "📬  Ответ сервера:"; \
	resp=$$($(GRPCURL) \
	  -H 'x-service-name: $(PUBLIC_SERVICE_NAME)' \
	  -H 'x-user-id: $(USER_ID)' \
	  -d "{\"user_id\":\"$$id\"}" \
	  $(GRPC_ADDR) place.v1.PlaceService/GetUserPlaces); \
	if echo "$$resp" | grep -qE '^[\[\{]'; then \
	    echo "$$resp" | jq .; \
	    echo "✅  Места получены"; \
	else \
	    echo "$$resp"; \
	    echo "❌  Что-то пошло не так..."; \
	fi

clean:
	@rm -f $(USER_FILE)
	@echo "🗑️   Файл $(USER_FILE) удалён"