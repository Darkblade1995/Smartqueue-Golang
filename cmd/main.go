package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"smartqueue/internal/api"
	"smartqueue/internal/auth"
	"smartqueue/internal/metrics"
	"smartqueue/internal/queue"
	"smartqueue/internal/retry"
	"smartqueue/internal/storage"
	"smartqueue/internal/worker"
)

func main() {
	// ── 1. config ─────────────────────────────────────────────
	redisAddr   := getEnv("REDIS_ADDR",   "localhost:6379")
	postgresURL := getEnv("POSTGRES_URL", "postgres://postgres:postgres@localhost:5433/smartqueue?sslmode=disable")
	httpPort    := getEnv("HTTP_PORT",    "8080")
	workerCount := 5

	log.Println("[main] iniciando SmartQueue...")

	// ── 2. Redis ──────────────────────────────────────────────
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	ctx := context.Background()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatalf("[main] no se pudo conectar a Redis: %v", err)
	}
	log.Println("[main] Redis conectado ✓")

	// ── 3. PostgreSQL ─────────────────────────────────────────
	store, err := storage.NewPostgresStore(postgresURL)
	if err != nil {
		log.Fatalf("[main] no se pudo conectar a PostgreSQL: %v", err)
	}

	if err := store.Migrate(); err != nil {
		log.Fatalf("[main] error en migración: %v", err)
	}
	log.Println("[main] PostgreSQL conectado y migrado ✓")

	// ── 4. Auth ───────────────────────────────────────────────
	db, err := sql.Open("postgres", postgresURL)
	if err != nil {
		log.Fatalf("[main] error abriendo conexión auth: %v", err)
	}

	authStore := auth.NewStore(db)
	if err := authStore.Migrate(); err != nil {
		log.Fatalf("[main] error migrando tabla api_keys: %v", err)
	}
	authMiddleware := auth.NewMiddleware(authStore)
	log.Println("[main] Auth configurado ✓")

	// ── 5. capas del sistema ──────────────────────────────────
	producer := queue.NewProducer(redisClient)
	consumer := queue.NewConsumer(redisClient)
	retryer  := retry.NewRetryer(producer, consumer, store)
	met      := metrics.New()

	// ── 6. worker pool ────────────────────────────────────────
	pool := worker.NewPool(workerCount, consumer, retryer, store, met)

	pool.Register("email", func(ctx context.Context, job *queue.Job) error {
		log.Printf("[handler] procesando email para: %v", job.Payload["to"])
		time.Sleep(100 * time.Millisecond)
		return nil
	})

	pool.Register("payment", func(ctx context.Context, job *queue.Job) error {
		log.Printf("[handler] procesando pago de: %v", job.Payload["amount"])
		time.Sleep(200 * time.Millisecond)
		return nil
	})

	pool.Register("report", func(ctx context.Context, job *queue.Job) error {
		log.Printf("[handler] generando reporte: %v", job.Payload["type"])
		time.Sleep(500 * time.Millisecond)
		return nil
	})

	pool.Register("failing", func(ctx context.Context, job *queue.Job) error {
		return fmt.Errorf("servicio caído simulado")
	})

	pool.Start(ctx)
	log.Printf("[main] %d workers corriendo ✓", workerCount)

	// ── 7. servidor HTTP ──────────────────────────────────────
	handler := api.NewHandler(producer, consumer, store, met, authStore)
	router  := api.NewRouter(handler, authMiddleware)

	server := &http.Server{
		Addr:         ":" + httpPort,
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("[main] servidor HTTP en puerto %s ✓", httpPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[main] error en servidor HTTP: %v", err)
		}
	}()

	// ── 8. graceful shutdown ──────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("[main] señal recibida, apagando...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[main] error en shutdown HTTP: %v", err)
	}

	pool.Stop()
	log.Println("[main] SmartQueue apagado limpiamente ✓")
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}