# :round_pushpin: Place Service

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

- Для методов, требующих идентификации через JWT-токен, необходимо передавать заголовок `x-user-id`.

</details>
<!-- markdownlint-enable MD033 -->

## :gear: Структура сервиса

:open_file_folder: **[./cmd](./cmd/)** - команды для запуска приложения.

:open_file_folder: **[./migrations](./migrations/)** - миграции для БД.

:open_file_folder: **[./internal/app](./internal/app/)** - сборка различных компонентов в единое приложение.

:open_file_folder: **[./internal/cache](./internal/cache/)** - абстракции для структуры кеша.

:open_file_folder: **[./internal/config](./internal/config/)** - загрузка файлов конфигурации.

:open_file_folder: **[./internal/domain](./internal/domain/)** - модели и события домена.

:open_file_folder: **[./internal/grpc]** - **gRPC-хендлеры** и gRPC-интерсепторы.

:open_file_folder: **[./internal/infrastructure](./internal/infrastructure/)** - конкретные реализации абстрактных сущностей (брокер сообщений, сериализатор), используемых для работы приложения.

:open_file_folder: **[./internal/lib](./internal/lib/)** - общие вспомогательные утилиты и функции.

:open_file_folder: **[./internal/outbox](./internal/outbox/)** - **outbox-процессор**, интерфейс продюсера для брокера сообщений, интерфейс сериализатора, структура сообщений и outbox-записей, список топиков.

:open_file_folder: **[./internal/service](./internal/service/)** - **сервисный слой (бизнес-логика)**.

:open_file_folder: **[./internal/storage](./internal/storage/)** - **слой хранения данных** (PostgreSQL, Redis).

## :desktop_computer: Локальный запуск и работа через терминал

### 1. Подготовка окружения

В вашем дистрибутиве должны быть установлены и готовы к работе:

- актуальная для проекта версия Go (см. [go.mod](./go.mod));

- сервер PostgreSQL (версия 13+) и утилита `psql`;

- сервер Redis (версия 7+);

- сервер Kafka (версия 4.0+);

- компилятор Protocol Buffers (`protoc`);

- утилита `grpcurl`;

- утилита `jq`;

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

Для переменных `DB_USER`, `DB_PASSWORD` и `DB_NAME` используйте значения из шага [3.1.](#31-postgresql).

Для переменной `KAFKA_PRODUCER_BROKERS` используйте значения из шага [3.2.](#32-kafka).

Для переменных `REDIS_CACHE_ADDR`, `REDIS_CACHE_PASSWORD`, `REDIS_CACHE_USERNAME` используйте значения из шага [3.3.](#33-redis). Примечание: сервис будет корректно работать и при пустых значениях `REDIS_CACHE_PASSWORD` и `REDIS_CACHE_USERNAME`.

### 4. Запуск и работа

Для удобства локальной работы в корне репозитория определён Makefile.

1. `make help` - узнайте о доступных командах.

2. `make protoset` - сгенерируйте protoset (обязательно перед локальной отправкой запросов).

3. `make run-svc` - примените миграции и запустите gRPC-сервер.

4. `CTRL + C` - отправьте серверу сигнал завершения, когда закончите работу.

В отдельном терминале перейдите в корневую папку репозитория и посылайте запросы на сервер.

- `make set-user` - установить ID конкретного пользователя. **Обязательная вспомогательная команда** для моделирования реальных запросов.

- `make add-place`, `make my-places` и `make places` - вызывайте gRPC-методы.

- `make clean` - выполните, чтобы удалить сохранённый ID пользователя.
