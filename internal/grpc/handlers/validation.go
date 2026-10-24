package handlersgrpc

import (
	"context"
	"regexp"
	"strings"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/alexgul25/place-svc/internal/grpc/interceptors"
)

// Ограничения длины считаются в символах, а не в байтах: кириллическая буква занимает два байта.
const (
	maxNameLength = 200
	maxInfoLength = 2000
)

var uuidRegexp = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// normalizeUUID проверяет, что строка - UUID в каноническом виде, и приводит её к нижнему регистру.
// PostgreSQL не различает регистр в UUID, а кеш и события используют ID как строку:
// без приведения "ABC..." и "abc..." попадали бы в разные ключи кеша.
func normalizeUUID(s string) (string, bool) {
	if !uuidRegexp.MatchString(s) {
		return "", false
	}
	return strings.ToLower(s), true
}

// requireUserID возвращает ID пользователя из заголовка x-user-id, который Gateway
// передаёт после проверки JWT. Без него или с некорректным значением вызов не аутентифицирован.
func requireUserID(ctx context.Context) (string, error) {
	raw, ok := interceptors.GetUserIDFromContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "user id is required")
	}

	userID, ok := normalizeUUID(raw)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "invalid user id")
	}

	return userID, nil
}

// validateID проверяет обязательное поле с ID пользователя и возвращает его в нижнем регистре.
func validateID(field, value string) (string, error) {
	if value == "" {
		return "", status.Errorf(codes.InvalidArgument, "%s is required", field)
	}

	id, ok := normalizeUUID(value)
	if !ok {
		return "", status.Errorf(codes.InvalidArgument, "%s must be a UUID", field)
	}

	return id, nil
}

// validateText проверяет текстовое поле. Обязательное поле не может состоять из одних пробелов.
// Символ NUL PostgreSQL не хранит в текстовых полях: запрос упал бы с внутренней ошибкой.
// Значение сохраняется как есть: пробелы по краям не обрезаются.
func validateText(field, value string, required bool, maxLength int) error {
	if required && strings.TrimSpace(value) == "" {
		return status.Errorf(codes.InvalidArgument, "%s is required", field)
	}
	if strings.ContainsRune(value, 0) {
		return status.Errorf(codes.InvalidArgument, "%s contains invalid characters", field)
	}
	if utf8.RuneCountInString(value) > maxLength {
		return status.Errorf(codes.InvalidArgument, "%s must be at most %d characters", field, maxLength)
	}
	return nil
}
