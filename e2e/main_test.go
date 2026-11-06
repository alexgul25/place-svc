//go:build integration

// Package e2e_test проверяет собранные бинарники сервиса и мигратора как чёрный ящик:
// настройки из переменных окружения, миграции, запуск, сигналы остановки и коды выхода.
package e2e_test

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/alexgul25/place-svc/internal/outbox"
	"github.com/alexgul25/place-svc/internal/testutil/kafkatest"
	"github.com/alexgul25/place-svc/internal/testutil/pgtest"
)

// Пути к собранным бинарникам, каталог для данных о покрытии и адреса Redis и Kafka.
var (
	serviceBin  string
	migratorBin string
	coverDir    string
	redisAddr   string
	brokers     []string
)

// redisDB - номер базы Redis для сервиса в E2E-тестах: другие тесты используют базы 1 и 2.
const redisDB = 3

func TestMain(m *testing.M) {
	// В TestMain флаги ещё не разобраны: test.gocoverdir нужен до m.Run.
	flag.Parse()
	os.Exit(run(m))
}

// run отделён от TestMain, чтобы отложенные вызовы выполнились до os.Exit.
func run(m *testing.M) int {
	if redisAddr = os.Getenv("TEST_REDIS_ADDR"); redisAddr == "" {
		fmt.Fprintln(os.Stderr, "TEST_REDIS_ADDR is not set, start a test Redis with `make test-infra-up`")
		return 1
	}
	var err error
	if brokers, err = kafkatest.Brokers(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	// Топик для событий при деплое создаёт kafka-init, здесь - тест.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := kafkatest.CreateTopic(ctx, brokers, outbox.TopicPlaceCreated); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	binDir, err := os.MkdirTemp("", "place-svc-e2e-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer os.RemoveAll(binDir)

	// Если тесты запущены с -test.gocoverdir, бинарники пишут покрытие туда же,
	// и в общий отчёт попадает код из cmd/, который выполняется только в них.
	if f := flag.Lookup("test.gocoverdir"); f != nil {
		coverDir = f.Value.String()
	}

	serviceBin = filepath.Join(binDir, "place-svc")
	migratorBin = filepath.Join(binDir, "migrator")
	for bin, pkg := range map[string]string{serviceBin: "../cmd/svc-starter", migratorBin: "../cmd/migrator"} {
		if err := build(bin, pkg); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	}

	return m.Run()
}

// build собирает бинарник. Если тесты собирают покрытие, бинарник тоже его собирает.
func build(bin, pkg string) error {
	args := []string{"build", "-o", bin}
	if coverDir != "" {
		args = append(args, "-cover", "-covermode="+testing.CoverMode(), "-coverpkg=github.com/alexgul25/place-svc/...")
	}
	args = append(args, pkg)

	out, err := exec.CommandContext(context.Background(), "go", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("go %s: %w\n%s", strings.Join(args, " "), err, out)
	}
	return nil
}

// ----------------------------------------------------------------------------
// Окружение
// ----------------------------------------------------------------------------

// env - переменные окружения для запуска бинарника.
type env map[string]string

// newEnv создаёт отдельную пустую базу и возвращает полный набор переменных сервиса,
// как в .env.example. Процессор проверяет outbox часто, чтобы тест не ждал событий долго.
func newEnv(t *testing.T) env {
	t.Helper()

	dsn, drop, err := pgtest.NewDatabase(t.Context(), "place_svc_e2e")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := drop(); err != nil {
			t.Error(err)
		}
	})

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("invalid DSN: %v", err)
	}
	password, ok := u.User.Password()
	if !ok || password == "" {
		// Сервис и мигратор не запускаются без DB_PASSWORD.
		t.Fatalf("%s must contain a password, like postgres://user:password@host:port/db", pgtest.EnvDSN)
	}

	return env{
		"ENV":                              "prod",
		"SERVICE_NAME":                     "place-svc",
		"DB_USER":                          u.User.Username(),
		"DB_PASSWORD":                      password,
		"DB_NAME":                          strings.TrimPrefix(u.Path, "/"),
		"DB_HOST":                          u.Hostname(),
		"DB_PORT":                          u.Port(),
		"GRPCSERVER_PORT":                  strconv.Itoa(freePort(t)),
		"GRACEFUL_TIMEOUT":                 "10s",
		"OUTBOX_PROCESSOR_SELECT_INTERVAL": "200ms",
		"KAFKA_PRODUCER_BROKERS":           strings.Join(brokers, ","),
		"KAFKA_PRODUCER_SEND_TIMEOUT":      "10s",
		"REDIS_CACHE_ADDR":                 redisAddr,
		"REDIS_CACHE_DB":                   strconv.Itoa(redisDB),
		"REDIS_CACHE_DIAL_TIMEOUT":         "2s",
		"REDIS_CACHE_TTL":                  "1h",
	}
}

// database возвращает из окружения только настройки БД: мигратору нужны лишь они.
func (e env) database() env {
	result := make(env)
	for k, v := range e {
		if strings.HasPrefix(k, "DB_") {
			result[k] = v
		}
	}
	return result
}

// with возвращает копию окружения с изменёнными переменными. Пустое значение удаляет переменную.
func (e env) with(kv ...string) env {
	result := make(env, len(e))
	for k, v := range e {
		result[k] = v
	}
	for i := 0; i+1 < len(kv); i += 2 {
		if kv[i+1] == "" {
			delete(result, kv[i])
		} else {
			result[kv[i]] = kv[i+1]
		}
	}
	return result
}

// freePort находит свободный TCP-порт.
func freePort(t *testing.T) int {
	t.Helper()

	var lc net.ListenConfig
	l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port: %v", err)
	}
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port
}

// ----------------------------------------------------------------------------
// Запуск бинарников
// ----------------------------------------------------------------------------

// syncBuffer собирает вывод процесса; безопасен для записи из нескольких горутин.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// process - запущенный бинарник.
type process struct {
	cmd    *exec.Cmd
	output *syncBuffer
	done   chan struct{}
	err    error
}

// start запускает бинарник с заданным окружением. Окружение родительского процесса
// не наследуется, а рабочая папка - временная: бинарник не подхватит посторонний .env.
func start(t *testing.T, bin string, e env) *process {
	t.Helper()

	// CommandContext завершит процесс, если тест закончится раньше него.
	cmd := exec.CommandContext(t.Context(), bin)
	cmd.Dir = t.TempDir()
	for k, v := range e {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	if coverDir != "" {
		cmd.Env = append(cmd.Env, "GOCOVERDIR="+coverDir)
	}

	p := &process{cmd: cmd, output: &syncBuffer{}, done: make(chan struct{})}
	cmd.Stdout, cmd.Stderr = p.output, p.output

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start %s: %v", filepath.Base(bin), err)
	}
	go func() {
		p.err = cmd.Wait()
		close(p.done)
	}()

	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("output of %s:\n%s", filepath.Base(bin), p.output.String())
		}
	})
	return p
}

// wait ждёт завершения процесса и возвращает код выхода.
func (p *process) wait(t *testing.T, timeout time.Duration) int {
	t.Helper()

	select {
	case <-p.done:
	case <-time.After(timeout):
		t.Fatalf("process did not exit within %v", timeout)
	}

	var exitErr *exec.ExitError
	switch {
	case p.err == nil:
		return 0
	case errors.As(p.err, &exitErr):
		return exitErr.ExitCode()
	default:
		t.Fatalf("process failed: %v", p.err)
		return -1
	}
}

// signal отправляет процессу сигнал.
func (p *process) signal(t *testing.T, sig syscall.Signal) {
	t.Helper()

	if err := p.cmd.Process.Signal(sig); err != nil {
		t.Fatalf("failed to send %v: %v", sig, err)
	}
}

// runToExit запускает бинарник и ждёт его завершения.
func runToExit(t *testing.T, bin string, e env) (exitCode int, output string) {
	t.Helper()

	p := start(t, bin, e)
	return p.wait(t, 30*time.Second), p.output.String()
}

// migrate применяет миграции и роняет тест при ошибке.
func migrate(t *testing.T, e env) {
	t.Helper()

	if code, out := runToExit(t, migratorBin, e); code != 0 {
		t.Fatalf("migrator exited with code %d:\n%s", code, out)
	}
}
