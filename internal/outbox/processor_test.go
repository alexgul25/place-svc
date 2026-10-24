package outbox_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/alexgul25/place-svc/internal/outbox"
)

// Тесты работают в пузыре synctest: время в нём виртуальное и идёт вперёд, только когда
// все горутины заблокированы, поэтому интервалы опроса из конфига не замедляют тесты,
// а synctest.Wait дожидается, пока процессор обработает всё, что может.
const (
	opTimeout      = 5 * time.Second
	selectInterval = 10 * time.Second
	selectSize     = 100
)

// ----------------------------------------------------------------------------
// Заглушки
// ----------------------------------------------------------------------------

// Поведение заглушек задаётся функциями в полях. Если функция не задана, а метод
// вызван, тест падает. Методы вызываются из горутины процессора, поэтому заглушки
// сообщают об ошибке через t.Errorf, а не t.Fatal.

type stubRepo struct {
	t               *testing.T
	selectPending   func(ctx context.Context, limit int) ([]outbox.Record, error)
	markAsPublished func(ctx context.Context, id string) error
}

func (r *stubRepo) SelectPending(ctx context.Context, limit int) ([]outbox.Record, error) {
	if r.selectPending == nil {
		r.t.Errorf("unexpected SelectPending(%d)", limit)
		return nil, errors.New("unexpected call")
	}
	return r.selectPending(ctx, limit)
}

func (r *stubRepo) MarkAsPublished(ctx context.Context, id string) error {
	if r.markAsPublished == nil {
		r.t.Errorf("unexpected MarkAsPublished(%q)", id)
		return errors.New("unexpected call")
	}
	return r.markAsPublished(ctx, id)
}

type stubProducer struct {
	t    *testing.T
	send func(ctx context.Context, messageID, topic string, key, value []byte) error
}

func (p *stubProducer) SendMessage(ctx context.Context, messageID, topic string, key, value []byte) error {
	if p.send == nil {
		p.t.Errorf("unexpected SendMessage(%q)", messageID)
		return errors.New("unexpected call")
	}
	return p.send(ctx, messageID, topic, key, value)
}

// Продюсер закрывает приложение после остановки процессора, а не сам процессор.
func (p *stubProducer) Close() error {
	p.t.Error("the processor must not close the producer")
	return nil
}

// ----------------------------------------------------------------------------
// Вспомогательные функции
// ----------------------------------------------------------------------------

// sentMessage - сообщение, которое процессор передал продюсеру.
type sentMessage struct {
	id, topic, key, value string
}

// recorder запоминает, что процессор делал с заглушками. Методы вызываются из горутины
// процессора, а тест читает результаты между тиками. Детектор гонок не видит порядка,
// который задаёт виртуальное время synctest, поэтому данные защищены мьютексом.
type recorder struct {
	mu        sync.Mutex
	selects   []int // limit каждого вызова SelectPending
	sent      []sentMessage
	published []string
	timeouts  []time.Duration // сколько времени оставалось у контекста каждой операции
}

// selectFrom возвращает SelectPending, который отдаёт пачки по одной на вызов, а потом пустые списки.
func (r *recorder) selectFrom(batches ...[]outbox.Record) func(context.Context, int) ([]outbox.Record, error) {
	return func(_ context.Context, limit int) ([]outbox.Record, error) {
		r.mu.Lock()
		defer r.mu.Unlock()

		r.selects = append(r.selects, limit)
		if len(batches) == 0 {
			return nil, nil
		}
		b := batches[0]
		batches = batches[1:]
		return b, nil
	}
}

func (r *recorder) send(_ context.Context, messageID, topic string, key, value []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.sent = append(r.sent, sentMessage{id: messageID, topic: topic, key: string(key), value: string(value)})
	return nil
}

func (r *recorder) markAsPublished(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.published = append(r.published, id)
	return nil
}

// rememberTimeout запоминает, сколько времени осталось до дедлайна контекста операции.
func (r *recorder) rememberTimeout(t *testing.T, ctx context.Context) {
	t.Helper()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Error("operation context has no deadline")
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.timeouts = append(r.timeouts, time.Until(deadline))
}

func (r *recorder) selectCalls() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.selects)
}

func (r *recorder) sentMessages() []sentMessage {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.sent)
}

func (r *recorder) sentIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	ids := make([]string, len(r.sent))
	for i, m := range r.sent {
		ids[i] = m.id
	}
	return ids
}

func (r *recorder) publishedIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.published)
}

func (r *recorder) operationTimeouts() []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.timeouts)
}

func record(id string) outbox.Record {
	return outbox.Record{
		ID: id,
		Message: outbox.Message{
			Topic:   outbox.TopicPlaceCreated,
			Key:     "user-of-" + id,
			Payload: []byte(`{"place_id":"` + id + `"}`),
		},
		SendStatus: outbox.StatusPending,
	}
}

// newProcessor собирает процессор с логгером, который пишет JSON-записи в буфер.
// Буфер можно читать после Shutdown.
func newProcessor(repo *stubRepo, producer *stubProducer) (*outbox.ProcessorWithSyncProducer, *bytes.Buffer) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	p := outbox.NewProcessorWithSyncProducer(repo, producer, log, opTimeout, selectInterval, selectSize)
	return p, &buf
}

// start запускает процессор, как приложение: Start в отдельной горутине.
// Возвращает канал, который закрывается, когда Start вернулся.
// Shutdown в t.Cleanup останавливает процессор, даже если тест упал раньше:
// иначе synctest.Test ждал бы его горутину бесконечно.
func start(t *testing.T, p *outbox.ProcessorWithSyncProducer) <-chan struct{} {
	t.Helper()

	done := make(chan struct{})
	go func() {
		p.Start()
		close(done)
	}()
	t.Cleanup(p.Shutdown)
	return done
}

// isClosed сообщает, закрыт ли канал.
func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func logRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()

	var result []map[string]any
	dec := json.NewDecoder(buf)
	for dec.More() {
		var rec map[string]any
		if err := dec.Decode(&rec); err != nil {
			t.Fatalf("failed to decode log record: %v", err)
		}
		result = append(result, rec)
	}
	return result
}

// findRecord ищет запись лога по сообщению и ID записи outbox.
func findRecord(recs []map[string]any, msg, recordID string) (map[string]any, bool) {
	for _, rec := range recs {
		if rec["msg"] == msg && (recordID == "" || rec["record_id"] == recordID) {
			return rec, true
		}
	}
	return nil, false
}

// checkErrorLogged проверяет запись об ошибке: уровень ERROR и текст ошибки.
func checkErrorLogged(t *testing.T, buf *bytes.Buffer, msg, recordID, wantErr string) {
	t.Helper()

	recs := logRecords(t, buf)
	rec, ok := findRecord(recs, msg, recordID)
	if !ok {
		t.Fatalf("no %q record for %q; log: %v", msg, recordID, recs)
	}
	if rec["level"] != "ERROR" || rec["error"] != wantErr {
		t.Errorf("%q: level = %v, error = %v, want ERROR and %q", msg, rec["level"], rec["error"], wantErr)
	}
}

// ----------------------------------------------------------------------------
// Обработка записей
// ----------------------------------------------------------------------------

func TestProcessor_PublishesPendingRecords(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := &recorder{}
		repo := &stubRepo{
			t:               t,
			selectPending:   rec.selectFrom([]outbox.Record{record("r1"), record("r2")}),
			markAsPublished: rec.markAsPublished,
		}
		p, _ := newProcessor(repo, &stubProducer{t: t, send: rec.send})
		start(t, p)

		// Первый опрос - через интервал после запуска.
		synctest.Wait()
		if n := len(rec.selectCalls()); n != 0 {
			t.Fatalf("SelectPending was called %d times before the first interval passed", n)
		}
		time.Sleep(selectInterval)
		synctest.Wait()

		want := []sentMessage{
			{id: "r1", topic: "place.created", key: "user-of-r1", value: `{"place_id":"r1"}`},
			{id: "r2", topic: "place.created", key: "user-of-r2", value: `{"place_id":"r2"}`},
		}
		if got := rec.sentMessages(); !slices.Equal(got, want) {
			t.Errorf("sent %+v, want %+v", got, want)
		}
		if got := rec.publishedIDs(); !slices.Equal(got, []string{"r1", "r2"}) {
			t.Errorf("published %q, want [r1 r2]", got)
		}
		if got := rec.selectCalls(); !slices.Equal(got, []int{selectSize}) {
			t.Errorf("SelectPending limits = %v, want [%d]", got, selectSize)
		}

		// Следующий опрос - ещё через интервал.
		time.Sleep(selectInterval)
		synctest.Wait()
		if n := len(rec.selectCalls()); n != 2 {
			t.Errorf("SelectPending was called %d times after two intervals, want 2", n)
		}
	})
}

// Запись, которую не удалось отправить, остаётся в outbox и уйдёт при следующем опросе.
// Остальные записи пачки отправляются.
func TestProcessor_SendErrorKeepsRecordPending(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := &recorder{}
		repo := &stubRepo{
			t:               t,
			selectPending:   rec.selectFrom([]outbox.Record{record("r1"), record("r2")}),
			markAsPublished: rec.markAsPublished,
		}
		producer := &stubProducer{t: t, send: func(ctx context.Context, id, topic string, key, value []byte) error {
			if id == "r1" {
				return errors.New("kafka is down")
			}
			return rec.send(ctx, id, topic, key, value)
		}}
		p, buf := newProcessor(repo, producer)
		start(t, p)

		time.Sleep(selectInterval)
		synctest.Wait()
		p.Shutdown()

		if got := rec.publishedIDs(); !slices.Equal(got, []string{"r2"}) {
			t.Errorf("published %q, want [r2]: a record that was not sent must stay pending", got)
		}
		checkErrorLogged(t, buf, "failed to send msg", "r1", "kafka is down")
	})
}

// Сообщение уже отправлено, поэтому ошибка отметки только записывается в лог:
// запись уйдёт повторно, а потребители отличают повторы по заголовку message_id.
func TestProcessor_MarkErrorIsLogged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := &recorder{}
		repo := &stubRepo{
			t:             t,
			selectPending: rec.selectFrom([]outbox.Record{record("r1"), record("r2")}),
			markAsPublished: func(ctx context.Context, id string) error {
				if id == "r1" {
					return errors.New("database is down")
				}
				return rec.markAsPublished(ctx, id)
			},
		}
		p, buf := newProcessor(repo, &stubProducer{t: t, send: rec.send})
		start(t, p)

		time.Sleep(selectInterval)
		synctest.Wait()
		p.Shutdown()

		if got := rec.sentIDs(); !slices.Equal(got, []string{"r1", "r2"}) {
			t.Errorf("sent %q, want [r1 r2]", got)
		}
		if got := rec.publishedIDs(); !slices.Equal(got, []string{"r2"}) {
			t.Errorf("published %q, want [r2]", got)
		}
		checkErrorLogged(t, buf, "failed to mark record as published", "r1", "database is down")
	})
}

func TestProcessor_SelectErrorIsRetried(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := &recorder{}
		selectNext := rec.selectFrom(nil, []outbox.Record{record("r1")})
		failed := false
		repo := &stubRepo{
			t: t,
			selectPending: func(ctx context.Context, limit int) ([]outbox.Record, error) {
				records, _ := selectNext(ctx, limit)
				// Первый вызов падает, второй отдаёт запись.
				if !failed {
					failed = true
					return nil, errors.New("database is down")
				}
				return records, nil
			},
			markAsPublished: rec.markAsPublished,
		}
		p, buf := newProcessor(repo, &stubProducer{t: t, send: rec.send})
		start(t, p)

		time.Sleep(selectInterval)
		synctest.Wait()
		if got := rec.sentIDs(); len(got) != 0 {
			t.Fatalf("sent %q after a failed select", got)
		}

		time.Sleep(selectInterval)
		synctest.Wait()
		p.Shutdown()

		if got := rec.publishedIDs(); !slices.Equal(got, []string{"r1"}) {
			t.Errorf("published %q, want [r1] after the next select", got)
		}
		checkErrorLogged(t, buf, "failed to select pending records", "", "database is down")
	})
}

// Каждая операция ограничена своим таймаутом OUTBOX_PROCESSOR_OP_TIMEOUT.
func TestProcessor_OperationTimeouts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Время в пузыре стоит, пока горутины работают, поэтому до дедлайна каждой
		// операции должно оставаться ровно opTimeout.
		rec := &recorder{}
		selectNext := rec.selectFrom([]outbox.Record{record("r1")})
		repo := &stubRepo{
			t: t,
			selectPending: func(ctx context.Context, limit int) ([]outbox.Record, error) {
				rec.rememberTimeout(t, ctx)
				return selectNext(ctx, limit)
			},
			markAsPublished: func(ctx context.Context, id string) error {
				rec.rememberTimeout(t, ctx)
				return rec.markAsPublished(ctx, id)
			},
		}
		producer := &stubProducer{t: t, send: func(ctx context.Context, id, topic string, key, value []byte) error {
			rec.rememberTimeout(t, ctx)
			return rec.send(ctx, id, topic, key, value)
		}}
		p, _ := newProcessor(repo, producer)
		start(t, p)

		time.Sleep(selectInterval)
		synctest.Wait()
		p.Shutdown()

		// SelectPending, SendMessage, MarkAsPublished.
		want := []time.Duration{opTimeout, opTimeout, opTimeout}
		if got := rec.operationTimeouts(); !slices.Equal(got, want) {
			t.Errorf("operation timeouts = %v, want %v", got, want)
		}
	})
}

// ----------------------------------------------------------------------------
// Запуск и остановка
// ----------------------------------------------------------------------------

func TestProcessor_ShutdownStopsPolling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := &recorder{}
		p, buf := newProcessor(&stubRepo{t: t, selectPending: rec.selectFrom()}, &stubProducer{t: t})
		done := start(t, p)

		time.Sleep(3 * selectInterval)
		synctest.Wait()
		if n := len(rec.selectCalls()); n != 3 {
			t.Fatalf("SelectPending was called %d times in three intervals, want 3", n)
		}

		p.Shutdown()
		synctest.Wait()
		if !isClosed(done) {
			t.Fatal("Start did not return after Shutdown")
		}

		time.Sleep(3 * selectInterval)
		if n := len(rec.selectCalls()); n != 3 {
			t.Errorf("SelectPending was called %d times after Shutdown", n-3)
		}

		recs := logRecords(t, buf)
		for _, msg := range []string{"processor with sync producer is started", "processor with sync producer is shutting down"} {
			if _, ok := findRecord(recs, msg, ""); !ok {
				t.Errorf("no %q record; log: %v", msg, recs)
			}
		}
	})
}

// Приложение запускает Start в горутине и при сигнале сразу вызывает Shutdown:
// Shutdown может опередить начало работы Start.
func TestProcessor_ShutdownRightAfterStart(t *testing.T) {
	for range 100 {
		synctest.Test(t, func(t *testing.T) {
			// SelectPending не задан: после Shutdown обращений к БД быть не должно.
			p, _ := newProcessor(&stubRepo{t: t}, &stubProducer{t: t})
			done := start(t, p)

			p.Shutdown()

			// Время идёт вперёд: работающий процессор опросил бы outbox.
			time.Sleep(3 * selectInterval)
			synctest.Wait()
			if !isClosed(done) {
				t.Fatal("Start did not return after Shutdown")
			}
		})
		if t.Failed() {
			return
		}
	}
}

// Если Shutdown вызван до Start, Start сразу возвращается и ничего не обрабатывает.
func TestProcessor_ShutdownBeforeStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, buf := newProcessor(&stubRepo{t: t}, &stubProducer{t: t})
		p.Shutdown()

		done := start(t, p)
		time.Sleep(3 * selectInterval)
		synctest.Wait()
		if !isClosed(done) {
			t.Fatal("Start is still running after an earlier Shutdown")
		}

		// Лог не должен сообщать о запуске процессора, который уже остановлен.
		if rec, ok := findRecord(logRecords(t, buf), "processor with sync producer is started", ""); ok {
			t.Errorf("unexpected record after Shutdown: %v", rec)
		}
	})
}

func TestProcessor_ShutdownTwice(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := &recorder{}
		p, _ := newProcessor(&stubRepo{t: t, selectPending: rec.selectFrom()}, &stubProducer{t: t})
		done := start(t, p)

		time.Sleep(selectInterval)
		p.Shutdown()
		p.Shutdown()

		synctest.Wait()
		if !isClosed(done) {
			t.Fatal("Start did not return after Shutdown")
		}
	})
}

// Остановка ждёт только запись, которая уже отправляется, а остальные записи пачки
// остаются в outbox до следующего запуска. Иначе при недоступной Kafka остановка
// длилась бы время отправки, умноженное на размер пачки.
func TestProcessor_ShutdownFinishesOnlyTheRecordInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := &recorder{}
		release := make(chan struct{})
		repo := &stubRepo{
			t:               t,
			selectPending:   rec.selectFrom([]outbox.Record{record("r1"), record("r2"), record("r3")}),
			markAsPublished: rec.markAsPublished,
		}
		producer := &stubProducer{t: t, send: func(ctx context.Context, id, topic string, key, value []byte) error {
			// Отправка зависает, пока тест её не отпустит.
			<-release
			return rec.send(ctx, id, topic, key, value)
		}}
		p, _ := newProcessor(repo, producer)
		done := start(t, p)

		// Процессор начал отправлять первую запись и ждёт.
		time.Sleep(selectInterval)
		synctest.Wait()

		// Shutdown запрашивает остановку и ждёт процессор: после synctest.Wait
		// обе горутины заблокированы, значит остановка уже запрошена.
		stopped := make(chan struct{})
		go func() {
			p.Shutdown()
			close(stopped)
		}()
		synctest.Wait()
		if isClosed(stopped) {
			t.Fatal("Shutdown returned while a record was being sent")
		}

		close(release)
		synctest.Wait()

		if !isClosed(stopped) || !isClosed(done) {
			t.Fatal("Shutdown did not return after the record in flight was sent")
		}
		if got := rec.sentIDs(); !slices.Equal(got, []string{"r1"}) {
			t.Errorf("sent %q after Shutdown was requested, want only the record in flight [r1]", got)
		}
		// Отправленная запись отмечается, иначе она ушла бы повторно.
		if got := rec.publishedIDs(); !slices.Equal(got, []string{"r1"}) {
			t.Errorf("published %q, want [r1]", got)
		}
	})
}
