package outbox

import (
	"context"
	"log/slog"
	"time"
)

type OutboxRepository interface {
	SelectPending(ctx context.Context, limit int) ([]Record, error)
	MarkAsPublished(ctx context.Context, id string) error
}

type ProcessorWithSyncProducer struct {
	repo           OutboxRepository
	producer       SyncMessageProducer
	log            *slog.Logger
	opTimeout      time.Duration
	selectInterval time.Duration
	selectSize     int
}

func NewProcessorWithSyncProducer(
	repo OutboxRepository,
	producer SyncMessageProducer,
	log *slog.Logger,
	opTimeout time.Duration,
	selectInterval time.Duration,
	selectSize int,
) *ProcessorWithSyncProducer {
	return &ProcessorWithSyncProducer{
		repo:           repo,
		producer:       producer,
		log:            log,
		opTimeout:      opTimeout,
		selectInterval: selectInterval,
		selectSize:     selectSize,
	}
}

func (p *ProcessorWithSyncProducer) Start(ctx context.Context) {
	const op = "ProcessorWithSyncProducer.Start"

	ticker := time.NewTicker(p.selectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.log.InfoContext(ctx, "processor with sync producer shutdown")

			return
		case <-ticker.C:
			selectCtx, selectCancel := context.WithTimeout(ctx, p.opTimeout)

			records, err := p.repo.SelectPending(selectCtx, p.selectSize)
			if err != nil {
				p.log.ErrorContext(selectCtx, "failed to select pending records", slog.String("source", op), slog.Any("error", err))
			}
			selectCancel()

			for _, record := range records {
				sendCtx, sendCancel := context.WithTimeout(ctx, p.opTimeout)

				err := p.producer.SendMessage(sendCtx, record.Message.Topic, []byte(record.Message.Key), record.Message.Payload)
				if err != nil {
					p.log.ErrorContext(
						sendCtx,
						"failed to send msg",
						slog.String("source", op), slog.String("record_id", record.ID), slog.Any("error", err),
					)
				} else {
					markCtx, markCancel := context.WithTimeout(ctx, p.opTimeout)

					err := p.repo.MarkAsPublished(markCtx, record.ID)
					if err != nil {
						p.log.ErrorContext(
							markCtx,
							"failed to mark record as published",
							slog.String("source", op), slog.String("record_id", record.ID), slog.Any("error", err),
						)
					}

					markCancel()
				}

				sendCancel()
			}
		}
	}
}
