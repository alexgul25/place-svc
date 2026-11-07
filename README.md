# :round_pushpin: Place Service

[![CI](https://github.com/alexgul25/place-svc/actions/workflows/ci.yml/badge.svg)](https://github.com/alexgul25/place-svc/actions/workflows/ci.yml)

Микросервис для проекта **Date Wishlist Hub**.

Ссылка на центральный репозиторий проекта: **[Date Wishlist Hub Deploy](https://github.com/alexgul25/date-wishlist-hub-deploy)**

Ссылка на канбан-доску проекта: **[Date Wishlist Hub - Development](https://github.com/users/alexgul25/projects/2)**

*Стек технологий сервиса:* `Go`  `gRPC`  `PostgreSQL`  `Kafka`  `Redis`

## :bulb: Описание сервиса

**Place Service** - внутренний gRPC-сервер, организующий логику работы с данными о списках мест пользователей.

- Protobuf-контракты определены публично в **[Protos](https://github.com/alexgul25/protos)**.
- При записи места в БД публикуется событие в брокер сообщений. Для согласованности данных **реализован паттерн Outbox**.
- Для снижения нагрузки на БД используется система кеширования с TTL-стратегией.
- В качестве брокера сообщений используется `Kafka`.
- В качестве БД используется `PostgreSQL`.
- В качесте кеша используется `Redis`.
- Методы **не должны** быть доступны пользователям напрямую (см. [архитектуру проекта](https://github.com/alexgul25/date-wishlist-hub-deploy#building_construction-архитектура-проекта)).

***Таблица gRPC-методов.***

| Method Name            | Auth | Calling service  | Info                                                                                |
| :--------------------: | :--: | :--------------: | ----------------------------------------------------------------------------------- |
| AddPlace               | ✅   | Gateway Service  | Добавление нового места                                                             |
| GetUserPlaces          | ✅   | Gateway Service  | Получение списка мест пользователя                                                  |

<!-- markdownlint-disable MD033 -->
<details>
<summary>Примечания</summary>

- Все запросы для **Place Service** должны передавать заголовок `x-service-name` - имя сервиса, вызывающего метод (Calling service).
- Заполненный столбец `Auth` указывает:
    1. вызов метода инициирован пользователем;
    2. ✅ и ❌ - соответственно нужен или не нужен JWT-токен для успешного вызова.
- Для методов, требующих идентификации через JWT-токен, необходимо передавать заголовок `x-user-id`. Без него или с некорректным значением сервис отвечает `Unauthenticated`.
- ID пользователя в заголовке `x-user-id` и в поле `user_id` - UUID, регистр букв не важен.
- В `AddPlace` поле `name` обязательно, не может состоять из одних пробелов и ограничено 200 символами, `info` - 2000 символами. Символ NUL в них недопустим. Значения сохраняются как есть, без обрезки пробелов. При нарушении сервис отвечает `InvalidArgument`.

</details>
<!-- markdownlint-enable MD033 -->

## :gear: Структура сервиса

:open_file_folder: **[./cmd](./cmd/)** - команды для запуска приложения.

:open_file_folder: **[./migrations](./migrations/)** - миграции для БД.

:open_file_folder: **[./e2e](./e2e/)** - E2E-тесты собранных бинарников сервиса и мигратора.

:open_file_folder: **[./scripts](./scripts/)** - смоук-тест Docker-образа.

:open_file_folder: **[./internal/app](./internal/app/)** - сборка различных компонентов в единое приложение.

:open_file_folder: **[./internal/cache](./internal/cache/)** - абстракции для структуры кеша.

:open_file_folder: **[./internal/config](./internal/config/)** - загрузка файлов конфигурации.

:open_file_folder: **[./internal/domain](./internal/domain/)** - модели и события домена.

:open_file_folder: **[./internal/grpc](./internal/grpc/)** - **gRPC-хендлеры** и gRPC-интерсепторы.

:open_file_folder: **[./internal/infrastructure](./internal/infrastructure/)** - конкретные реализации абстрактных сущностей (брокер сообщений, сериализатор), используемых для работы приложения.

:open_file_folder: **[./internal/lib](./internal/lib/)** - общие вспомогательные утилиты и функции.

:open_file_folder: **[./internal/outbox](./internal/outbox/)** - **outbox-процессор**, интерфейс продюсера для брокера сообщений, интерфейс сериализатора, структура сообщений и outbox-записей, список топиков.

:open_file_folder: **[./internal/service](./internal/service/)** - **сервисный слой (бизнес-логика)**.

:open_file_folder: **[./internal/storage](./internal/storage/)** - **слой хранения данных** (PostgreSQL, Redis).

:open_file_folder: **[./internal/testutil](./internal/testutil/)** - общий код для интеграционных тестов (временные базы PostgreSQL, топики Kafka).

:open_file_folder: **[./.github](./.github/)** - CI на GitHub Actions, настройки Dependabot и шаблоны issue.

## :desktop_computer: Локальный запуск и работа через терминал

В данном разделе приведена инструкция по запуску одного **Place Service**. Сервис можно запустить двумя способами: собрать и запустить бинарник локально или запустить в Docker-контейнере.

Инструкция по запуску всего проекта целиком доступна по **[ссылке](https://github.com/alexgul25/date-wishlist-hub-deploy#desktop_computer-локальный-запуск-и-работа-через-терминал)**.

### 1. Подготовка окружения

В вашем дистрибутиве должны быть установлены и готовы к работе:

- актуальная для проекта версия Go (см. [go.mod](./go.mod)) - для локального запуска;
- Docker Engine с плагином `buildx` - для запуска в контейнере, интеграционных тестов и проверки образа;
- сервер PostgreSQL (версия 15+) и утилита `psql`;
- сервер Redis (версия 7.2+);
- сервер Kafka (версия 4.0+);
- компилятор Protocol Buffers (`protoc`);
- утилита `grpcurl`;
- утилита `jq`;
- компилятор `gcc` (пакет `build-essential`) - для тестов с детектором гонок;
- утилита `golangci-lint` (v2) - для проверки кода;
- утилита `make`.

### 2. Клонирование нужных репозиториев

***ВАЖНО!*** Репозитории должны быть клонированы **в одну и ту же папку**.

- Клонируйте этот репозиторий c помощью HTTP или SSH.

```bash
git clone https://github.com/alexgul25/place-svc.git
```

```bash
git clone git@github.com:alexgul25/place-svc.git
```

- Клонируйте репозиторий **[Protos](https://github.com/alexgul25/protos)** с помощью HTTP или SSH. С его помощью будет сгенерирован protoset-файл, необходимый для отправки gRPC-запросов через терминал (он нужен, поскольку Place Service не поддерживает reflection).

```bash
git clone https://github.com/alexgul25/protos.git
```

```bash
git clone git@github.com:alexgul25/protos.git
```

### 3. Настройка инфраструктуры

#### 3.1. PostgreSQL

Запустите сервер PostgreSQL, затем создайте пользователя и базу данных для **Place Service**.

```bash
sudo -u postgres psql -c "CREATE USER <имя пользователя> WITH PASSWORD '<пароль>';"
```

```bash
sudo -u postgres psql -c "CREATE DATABASE <имя БД> OWNER <имя пользователя>;"
```

Проверьте доступ.

```bash
psql -h localhost -U <имя пользователя> -d <имя БД> -c "SELECT 1;"
```

Если всё работает корректно, вы увидите следующий вывод:

```bash
 ?column? 
----------
        1
(1 row)
```

#### 3.2. Kafka

Запустите сервер Kafka. Если у вас отключено автоматическое создание топиков, создайте их самостоятельно (названия топиков см. в **[topics.go](./internal/outbox/topics.go)**).

#### 3.3. Redis

Запустите сервер Redis. Затем можно создать пользователя и пароль для **Place Service**, но при локальной работе это необязательно :grin:.

#### 3.4. Файл конфигурации

***ВАЖНО!*** Создайте в корневой папке репозитория файл `.env` для переменных окружения и заполните его (см [.env.example](.env.example)).

Для переменных `DB_USER`, `DB_PASSWORD` и `DB_NAME` используйте значения из шага [3.1.](#31-postgresql)

Для переменной `KAFKA_PRODUCER_BROKERS` используйте значения из шага [3.2.](#32-kafka)

Для переменных `REDIS_CACHE_ADDR`, `REDIS_CACHE_PASSWORD`, `REDIS_CACHE_USERNAME` используйте значения из шага [3.3.](#33-redis) Примечание: сервис будет корректно работать и при пустых значениях `REDIS_CACHE_PASSWORD` и `REDIS_CACHE_USERNAME`.

Мигратору нужны только переменные `DB_*`: остальные он не читает.

<!-- markdownlint-disable MD033 -->
<details>
<summary>Особенности .env при запуске в Docker</summary>

- Значения указывайте без кавычек: Docker передаёт их в контейнер как есть, вместе с кавычками.
- `localhost` в `DB_HOST`, `KAFKA_PRODUCER_BROKERS` и `REDIS_CACHE_ADDR` внутри контейнера означает сам контейнер, а не вашу машину (см. подсказки к варианту запуска в Docker в [следующем шаге](#4-запуск-и-работа)).

</details>
<!-- markdownlint-enable MD033 -->

### 4. Запуск и работа

Для удобства локальной работы в корне репозитория определён Makefile.

1. `make help` - узнайте о доступных командах.
2. `make protoset` - сгенерируйте protoset (обязательно перед локальной отправкой запросов).

#### Вариант 1. Локальный запуск

1. `make run` - примените миграции, соберите бинарник и запустите gRPC-сервер.
2. `CTRL + C` - отправьте серверу сигнал завершения, когда закончите работу.

#### Вариант 2. Запуск в Docker

В корне репозитория определены **[Dockerfile](./Dockerfile)** и **[.dockerignore](./.dockerignore)**. Итоговый образ содержит бинарник сервиса и бинарник мигратора (SQL-миграции встроены в него), переменные окружения передаются в контейнер при запуске.

1. `docker build -t place-svc .` - соберите образ.
2. `docker run --rm --network host --env-file .env --entrypoint /app/migrator place-svc` - примените миграции.
3. `docker run --rm --name place-svc --network host --env-file .env place-svc` - запустите контейнер с gRPC-сервером.
4. `CTRL + C` или `docker stop place-svc` из другого терминала - отправьте серверу сигнал завершения, когда закончите работу.

<!-- markdownlint-disable MD033 -->
<details>
<summary>Подсказки</summary>

- Флаг `--network host` запускает контейнер в сети вашей машины: `localhost` в `DB_HOST`, `KAFKA_PRODUCER_BROKERS` и `REDIS_CACHE_ADDR` указывает на локальные PostgreSQL, Kafka и Redis, а gRPC-сервер доступен на `localhost:<GRPCSERVER_PORT>`, поэтому команды Makefile для отправки запросов работают без изменений. Режим работает в Docker Engine на Linux (в том числе в WSL2).
- Если PostgreSQL, Kafka и Redis доступны контейнеру по сети (например, запущены в других контейнерах), вместо `--network host` опубликуйте порт сервера: `-p <порт>:<порт>`, где порт совпадает с `GRPCSERVER_PORT`, и укажите в `.env` сетевые адреса этих сервисов.
- Если при сборке не удаётся скачать Go-модули (например, `proxy.golang.org` недоступен), передайте другой прокси через аргумент сборки: `docker build --build-arg GOPROXY=https://goproxy.io,direct -t place-svc .`
- Чтобы запустить контейнер в фоне, замените `--rm` на `-d`. Логи сервиса можно посмотреть командой `docker logs -f place-svc`, остановить и удалить контейнер - командами `docker stop place-svc` и `docker rm place-svc`.
- Проверить состояние запущенного сервиса можно командой `docker exec place-svc /bin/grpc_health_probe -addr=:<порт>`, где порт совпадает с `GRPCSERVER_PORT`. Сервис отвечает по стандартному протоколу gRPC Health Checking.
- При остановке сервис ждёт завершения начатых запросов и outbox-процессора не дольше `GRACEFUL_TIMEOUT`, затем закрывает оставшиеся соединения. Этот таймаут должен быть меньше времени, которое `docker stop` ждёт до принудительного завершения (по умолчанию 10 секунд), например: `docker stop -t 15 place-svc`. Если сервис ещё ждёт PostgreSQL или Kafka при запуске, он останавливается сразу.

</details>
<!-- markdownlint-enable MD033 -->

#### Работа с API

В отдельном терминале перейдите в корневую папку репозитория и посылайте запросы на сервер.

- `make set-user` - установить ID конкретного пользователя. **Обязательная вспомогательная команда** для моделирования реальных запросов.
- `make add-place`, `make my-places` и `make places` - вызывайте gRPC-методы.
- `make clean` - выполните, чтобы удалить сохранённые бинарник, ID пользователя и отчёты о покрытии.

#### Проверка кода

- `make test` - запустите unit-тесты с детектором гонок (PostgreSQL, Redis и Kafka не нужны).
- `make test-integration` - запустите все тесты, включая интеграционные и E2E: PostgreSQL, Redis и Kafka для них поднимаются в Docker-контейнерах на портах `55433`, `56379` и `59092`.
- `make test-cover` - соберите отчёт о покрытии всеми тестами, включая код, выполненный в собранных бинарниках (`coverage.html`), и проверьте порог покрытия (см. [Тестирование и CI](#test_tube-тестирование-и-ci)).
- `make test-infra-down` - остановите и удалите контейнеры с тестовыми PostgreSQL, Redis и Kafka.
- `make docker-smoke` - соберите Docker-образ и проверьте его: миграции, запуск, health-check, остановка.
- `make lint` - запустите линтеры.
- `make ci` - запустите локально все проверки CI, кроме проверки Docker-образа.

<!-- markdownlint-disable MD033 -->
<details>
<summary>Интеграционные тесты</summary>

- Тесты помечены build-тегом `integration`. К PostgreSQL они подключаются по строке из переменной окружения `TEST_DATABASE_URL`, к Redis - по адресу из `TEST_REDIS_ADDR`, к Kafka - по списку брокеров из `TEST_KAFKA_BROKERS`.
- Для каждого тестового пакета в PostgreSQL создаётся отдельная временная база, после тестов она удаляется. Пользователю из `TEST_DATABASE_URL` нужно право создавать базы (`CREATEDB`), а в строке подключения должен быть пароль.
- Redis тесты не очищают: каждый тест работает со своими ключами в базах 1-3 и удаляет их.
- Тестовая Kafka, как и при деплое, не создаёт топики при первом обращении: тесты создают их сами. Тест продюсера пишет в свой временный топик и удаляет его, тесты приложения и E2E - в `place.created`.
- E2E-тесты (папка [e2e](./e2e/)) собирают бинарники сервиса и мигратора и проверяют их как чёрный ящик: настройки из окружения, коды выхода, остановку по сигналам, формат логов.
- Вместо контейнеров можно указать свои серверы: `make test-integration TEST_DATABASE_URL=postgres://... TEST_REDIS_ADDR=host:port TEST_KAFKA_BROKERS=host:port`. Redis при этом нужен отдельный, а не общий с другими приложениями: один из тестов на 2 секунды приостанавливает обработку команд (`CLIENT PAUSE`), другой создаёт временного пользователя ACL.
- Чтобы VS Code (gopls) анализировал файлы интеграционных тестов, добавьте в настройки `"go.buildTags": "integration"`.

</details>
<!-- markdownlint-enable MD033 -->

## :test_tube: Тестирование и CI

Сервис тестируется как самостоятельный компонент: остальные сервисы проекта для тестов не нужны.

***Уровни тестов.***

| Уровень | Где | Что проверяется | Команда |
| --- | --- | --- | --- |
| Unit | `internal/config`, `internal/lib`, `internal/cache`, `internal/grpc/interceptors`, `internal/service`, `internal/outbox`, `internal/infrastructure` | Конфигурация, логгер, ключи кеша, интерсепторы, сериализатор, бизнес-логика и outbox-процессор с заглушками, продюсер Kafka на `sarama.MockBroker` | `make test` |
| Компонентные | `internal/grpc/handlers`, `internal/app/grpc`, `internal/app` | gRPC API через сервер в памяти (bufconn), health-check, запуск и корректная остановка приложения с заглушками вместо PostgreSQL, Redis и Kafka | `make test` |
| Интеграционные | `internal/storage`, `internal/infrastructure/kafka`, `migrations` | SQL-запросы и миграции на настоящем PostgreSQL, кеш на настоящем Redis, продюсер на настоящей Kafka | `make test-integration` |
| Полный стек | `internal/app` | Приложение, собранное как в `main`: пользовательский сценарий через gRPC, кеш в Redis, события в Kafka через outbox | `make test-integration` |
| E2E | `e2e` | Собранные бинарники сервиса и мигратора: окружение, коды выхода, сигналы, формат логов | `make test-integration` |
| Docker | `scripts/docker-smoke.sh` | Образ: миграции, запуск, health-check, остановка | `make docker-smoke` |

***Задачи CI*** ([ci.yml](./.github/workflows/ci.yml)). CI запускается на каждый PR и пуш в `main`, раз в неделю по расписанию и вручную. Для слияния PR в `main` все задачи должны пройти.

| Задача | Что делает |
| --- | --- |
| Lint | Проверяет `go.mod` и `go.sum` (`go mod tidy`) и запускает линтеры из [.golangci.yml](./.golangci.yml) |
| Vulnerabilities | Ищет известные уязвимости в зависимостях и стандартной библиотеке (`govulncheck`) |
| Unit tests | Собирает пакеты и запускает тесты без PostgreSQL, Redis и Kafka с детектором гонок |
| Integration tests (oldest, newest) | Запускает все тесты с покрытием (`make test-cover`) на младших (PostgreSQL 15, Redis 7.2, Kafka 4.0) и старших (PostgreSQL 18, Redis 8.10, Kafka 4.3) поддерживаемых версиях серверов |
| Docker image | Собирает образ и проверяет его тем же скриптом, что и `make docker-smoke` |

***Покрытие.*** `make test-cover` считает общее покрытие всеми тестами, включая код, выполненный внутри собранных бинарников в E2E-тестах. Команда и CI падают, если покрытие ниже порога `COVER_MIN` из [Makefile](./Makefile) (95%) или если какой-то пакет сервиса не попал в отчёт: пакет, который не использует ни один тест, иначе не учитывался бы вовсе. Итог покрытия виден на странице прогона CI (Summary), HTML-отчёт сохраняется в артефакте `coverage`.

***Обновление зависимостей.*** [Dependabot](./.github/dependabot.yml) по понедельникам открывает PR с обновлениями Go-модулей, GitHub Actions и образа `grpc-health-probe`. Новые версии предлагаются не раньше чем через неделю после выхода, исправления уязвимостей - сразу. Вручную обновляются версия Go (`go.mod` и `GO_VERSION` в Dockerfile), версия `golangci-lint` в CI и версии PostgreSQL, Redis и Kafka: в матрице интеграционных тестов и задаче Docker image в CI, а также в `TEST_DB_IMAGE`, `TEST_REDIS_IMAGE` и `TEST_KAFKA_IMAGE` в Makefile. Об уязвимостях в стандартной библиотеке Go сообщит еженедельный прогон `govulncheck`.

***Безопасность репозитория.*** В настройках репозитория (Settings → Advanced Security) включены:

- Dependabot alerts и Dependabot security updates - оповещения об уязвимых зависимостях и PR с исправлением сразу после публикации уязвимости;
- CodeQL (default setup) - поиск уязвимостей в коде на каждый PR и пуш в `main` и раз в неделю, результаты во вкладке Security;
- Secret scanning и push protection - поиск токенов и ключей в репозитории, пуш с ними блокируется.
