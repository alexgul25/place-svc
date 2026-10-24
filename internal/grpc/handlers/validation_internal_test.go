package handlersgrpc

import (
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Внутренний тест: функции валидации не экспортируются. Тесты хэндлеров проверяют,
// какой код получает клиент, а здесь перебираются варианты строк, которые нужно
// принять или отклонить. Через gRPC каждый такой вариант стоил бы отдельного запуска сервера.

func TestNormalizeUUID(t *testing.T) {
	const canonical = "4f1c2d3e-5a6b-4c7d-8e9f-0a1b2c3d4e5f"

	valid := []struct {
		name string
		in   string
		want string
	}{
		{name: "canonical lower case", in: canonical, want: canonical},
		{name: "upper case", in: strings.ToUpper(canonical), want: canonical},
		{name: "mixed case", in: "4F1c2D3e-5A6b-4C7d-8E9f-0A1b2C3d4E5f", want: canonical},
		{name: "nil UUID", in: "00000000-0000-0000-0000-000000000000", want: "00000000-0000-0000-0000-000000000000"},
		{name: "max UUID", in: "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF", want: "ffffffff-ffff-ffff-ffff-ffffffffffff"},
	}

	for _, tt := range valid {
		t.Run("valid/"+tt.name, func(t *testing.T) {
			got, ok := normalizeUUID(tt.in)
			if !ok || got != tt.want {
				t.Errorf("normalizeUUID(%q) = (%q, %v), want (%q, true)", tt.in, got, ok, tt.want)
			}
		})
	}

	invalid := []struct {
		name string
		in   string
	}{
		{name: "empty", in: ""},
		{name: "prefix before UUID", in: "x" + canonical},
		{name: "suffix after UUID", in: canonical + "x"},
		{name: "leading space", in: " " + canonical},
		{name: "trailing newline", in: canonical + "\n"},
		{name: "braces", in: "{" + canonical + "}"},
		{name: "URN prefix", in: "urn:uuid:" + canonical},
		{name: "no hyphens", in: strings.ReplaceAll(canonical, "-", "")},
		{name: "one hyphen missing", in: strings.Replace(canonical, "-", "", 1)},
		{name: "hyphen in wrong position", in: "4f1c2d3-e5a6b-4c7d-8e9f-0a1b2c3d4e5f"},
		{name: "non-hex letter in first group", in: "gf1c2d3e-5a6b-4c7d-8e9f-0a1b2c3d4e5f"},
		{name: "non-hex letter in last group", in: "4f1c2d3e-5a6b-4c7d-8e9f-0a1b2c3d4e5z"},
		// Кириллическая «е» выглядит как латинская e, но это другой символ.
		{name: "cyrillic look-alike letter", in: "4f1c2d3е-5a6b-4c7d-8e9f-0a1b2c3d4e5f"},
		{name: "extra group", in: canonical + "-0000"},
	}

	for _, tt := range invalid {
		t.Run("invalid/"+tt.name, func(t *testing.T) {
			if got, ok := normalizeUUID(tt.in); ok {
				t.Errorf("normalizeUUID(%q) = (%q, true), want rejection", tt.in, got)
			}
		})
	}
}

func TestValidateText(t *testing.T) {
	const maxLength = 5

	tests := []struct {
		name     string
		value    string
		required bool
		wantMsg  string // пусто - значение допустимо
	}{
		{name: "required value", value: "Парк", required: true},
		{name: "optional empty value", value: "", required: false},
		{name: "optional value of spaces", value: "   ", required: false},
		{name: "spaces around text", value: " Парк", required: true},
		{name: "required empty value", value: "", required: true, wantMsg: "field is required"},
		{name: "required value of spaces", value: "  ", required: true, wantMsg: "field is required"},
		// TrimSpace убирает и другие пробельные символы Unicode, например неразрывный пробел.
		{name: "required value of non-breaking spaces", value: "  ", required: true, wantMsg: "field is required"},
		{name: "NUL inside", value: "Па\x00рк", required: true, wantMsg: "field contains invalid characters"},
		{name: "only NUL", value: "\x00", required: false, wantMsg: "field contains invalid characters"},
		// Длина - в символах: 5 кириллических букв занимают 10 байт.
		{name: "exactly max characters", value: "яяяяя", required: true},
		{name: "one character over max", value: "яяяяяя", required: true, wantMsg: "field must be at most 5 characters"},
		{name: "ASCII over max", value: "abcdef", required: false, wantMsg: "field must be at most 5 characters"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateText("field", tt.value, tt.required, maxLength)

			if tt.wantMsg == "" {
				if err != nil {
					t.Errorf("validateText(%q) returned %v, want nil", tt.value, err)
				}
				return
			}

			st, ok := status.FromError(err)
			if !ok || st.Code() != codes.InvalidArgument || st.Message() != tt.wantMsg {
				t.Errorf("validateText(%q) = %v, want InvalidArgument %q", tt.value, err, tt.wantMsg)
			}
		})
	}
}
