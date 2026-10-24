package outbox

import (
	"context"
	"log/slog"
	"sync"
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

	mu       sync.Mutex
	started  bool
	stopping bool
	done     chan struct{} // закрывается в Shutdown: обработку пора остановить
	stopped  chan struct{} // закрывается, когда Start вернулся
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
		done:           make(chan struct{}),
		stopped:        make(chan struct{}),
	}
}

// Start обрабатывает outbox, пока не вызван Shutdown. Приложение вызывает его
// в отдельной горутине. Если Shutdown уже вызван, Start сразу возвращается.
func (p *ProcessorWithSyncProducer) Start() {
	const op = "ProcessorWithSyncProducer.Start"

	// Запуск и остановка согласованы под мьютексом: Shutdown, вызванный сразу
	// после go Start(), либо дождётся обработки, либо Start увидит остановку и не начнёт её.
	p.mu.Lock()
	if p.started || p.stopping {
		p.mu.Unlock()
		return
	}
	p.started = true
	p.mu.Unlock()
	defer close(p.stopped)

	p.log.Info("processor with sync producer is started", slog.String("source", op))

	ticker := time.NewTicker(p.selectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
			selectCtx, selectCancel := context.WithTimeout(context.Background(), p.opTimeout)

			records, err := p.repo.SelectPending(selectCtx, p.selectSize)
			if err != nil {
				p.log.ErrorContext(selectCtx, "failed to select pending records", slog.String("source", op), slog.Any("error", err))
			}
			selectCancel()

			for _, record := range records {

				// После Shutdown новые записи не отправляются: они останутся в outbox
				// до следующего запуска. Запись, которая уже отправляется, дописывается.
				if p.stopRequested() {
					return
				}

				sendCtx, sendCancel := context.WithTimeout(context.Background(), p.opTimeout)

				err := p.producer.SendMessage(sendCtx, record.ID, record.Message.Topic, []byte(record.Message.Key), record.Message.Payload)
				if err != nil {
					p.log.ErrorContext(
						sendCtx,
						"failed to send msg",
						slog.String("source", op), slog.String("record_id", record.ID), slog.Any("error", err),
					)
				} else {
					markCtx, markCancel := context.WithTimeout(context.Background(), p.opTimeout)

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

// stopRequested сообщает, вызван ли Shutdown.
func (p *ProcessorWithSyncProducer) stopRequested() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// Shutdown останавливает обработку и ждёт, пока Start вернётся.
// Повторный вызов ничего не делает.
func (p *ProcessorWithSyncProducer) Shutdown() {
	p.mu.Lock()
	if p.stopping {
		p.mu.Unlock()
		return
	}
	p.stopping = true
	started := p.started
	close(p.done)
	p.mu.Unlock()

	p.log.Info("processor with sync producer is shutting down")

	if started {
		<-p.stopped
	}

	p.log.Info("processor with sync producer is shutdowned successfully")
}
