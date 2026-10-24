package handlersgrpc

import (
	"strings"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Ограничения длины считаются в символах, а не в байтах: кириллическая буква занимает два байта.
const (
	maxNameLength = 200
	maxInfoLength = 2000
)

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
