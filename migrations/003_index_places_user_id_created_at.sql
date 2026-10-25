-- +goose Up
-- Список мест пользователя ищется по user_id: без индекса PostgreSQL читал всю таблицу,
-- то есть места всех пользователей. created_at - второй столбец, потому что список
-- сортируется по нему.
-- Пока индекс строится, запись в places заблокирована: на миллионе мест это около
-- полусекунды. Если таблица большая, индекс можно заранее построить вручную тем же
-- запросом с CREATE INDEX CONCURRENTLY, тогда миграция его пропустит.
CREATE INDEX IF NOT EXISTS idx_places_user_id_created_at ON places (user_id, created_at);

-- +goose Down
DROP INDEX IF EXISTS idx_places_user_id_created_at;
