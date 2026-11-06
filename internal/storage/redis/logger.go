package redis

import (
	"context"
	"fmt"
	"log/slog"

	goredis "github.com/redis/go-redis/v9"
)

// SetLogger направляет сообщения go-redis в лог сервиса. Сам go-redis пишет их в stderr
// обычным текстом (например, «connection pool: failed to dial after 5 attempts»),
// и в окружении prod такие строки оказывались среди JSON-записей.
// Настройка глобальная и действует на все клиенты go-redis в процессе. Вызывать SetLogger
// нужно до создания клиентов: go-redis читает логгер без синхронизации.
func SetLogger(log *slog.Logger) {
	goredis.SetLogger(logAdapter{log: log})
}

type logAdapter struct {
	log *slog.Logger
}

// Printf пишет сообщение go-redis предупреждением: об ошибке операции сервис
// сообщает сам, а go-redis добавляет подробности.
func (a logAdapter) Printf(ctx context.Context, format string, v ...any) {
	a.log.WarnContext(ctx, fmt.Sprintf(format, v...), slog.String("source", "go-redis"))
}
