package interceptors

import (
	"context"
	"log/slog"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	HeaderServiceName = "x-service-name"
	HeaderUserID      = "x-user-id"
)

func NewRecoveryInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	recoveryOpts := []recovery.Option{
		recovery.WithRecoveryHandler(func(p any) (err error) {
			log.Error("recovered from panic", slog.Any("panic", p))

			return status.Error(codes.Internal, "internal error")
		}),
	}

	return recovery.UnaryServerInterceptor(recoveryOpts...)
}

func interceptorLogger(log *slog.Logger) logging.Logger {
	return logging.LoggerFunc(func(ctx context.Context, level logging.Level, msg string, fields ...any) {
		var lvl slog.Level
		switch level {
		case logging.LevelInfo:
			lvl = slog.LevelInfo
		case logging.LevelDebug:
			lvl = slog.LevelDebug
		case logging.LevelWarn:
			lvl = slog.LevelWarn
		case logging.LevelError:
			lvl = slog.LevelError
		default:
			lvl = slog.LevelInfo
		}

		log.Log(ctx, lvl, msg, fields...)
	})
}

func NewLoggingInterceptor(log *slog.Logger, headersToLog []string) grpc.UnaryServerInterceptor {
	loggingOpts := []logging.Option{
		logging.WithFieldsFromContext(func(ctx context.Context) logging.Fields {
			md, ok := metadata.FromIncomingContext(ctx)
			if !ok {
				return nil
			}

			fields := logging.Fields{}
			for _, header := range headersToLog {
				if values := md.Get(header); len(values) != 0 {
					fields = append(fields, header, values[0])
				}
			}

			return fields
		}),
	}

	return logging.UnaryServerInterceptor(interceptorLogger(log), loggingOpts...)
}

func NewContextEnricherInterceptor(headersToEnrich []string) grpc.UnaryServerInterceptor {
	return grpc.UnaryServerInterceptor(
		func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
			md, ok := metadata.FromIncomingContext(ctx)
			if ok {
				for _, header := range headersToEnrich {
					if values := md.Get(header); len(values) > 0 && values[0] != "" {
						ctx = context.WithValue(ctx, ctxKey(header), values[0])
					}
				}
			}

			return handler(ctx, req)
		},
	)
}

type ctxKey string

func GetServiceNameFromContext(ctx context.Context) (string, bool) {
	res, ok := ctx.Value(ctxKey(HeaderServiceName)).(string)
	return res, ok
}

func GetUserIDFromContext(ctx context.Context) (string, bool) {
	res, ok := ctx.Value(ctxKey(HeaderUserID)).(string)
	return res, ok
}
