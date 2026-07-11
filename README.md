```
███████╗███╗   ███╗ █████╗ ██████╗ ████████╗ ██████╗ ██╗   ██╗███████╗██╗   ██╗███████╗
██╔════╝████╗ ████║██╔══██╗██╔══██╗╚══██╔══╝██╔═══██╗██║   ██║██╔════╝██║   ██║██╔════╝
███████╗██╔████╔██║███████║██████╔╝   ██║   ██║   ██║██║   ██║█████╗  ██║   ██║█████╗  
╚════██║██║╚██╔╝██║██╔══██║██╔══██╗   ██║   ██║▄▄ ██║██║   ██║██╔══╝  ██║   ██║██╔══╝  
███████║██║ ╚═╝ ██║██║  ██║██║  ██║   ██║   ╚██████╔╝╚██████╔╝███████╗╚██████╔╝███████╗
╚══════╝╚═╝     ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝    ╚══▀▀═╝  ╚═════╝ ╚══════╝ ╚═════╝ ╚══════╝
```

# SmartQueue — Distributed Job Queue Engine

A production-grade distributed job queue system built from scratch in Go — no frameworks, no magic.
Every component was designed, implemented, and debugged by hand: from a concurrent worker pool with goroutines to an exponential backoff retry engine with Dead Letter Queue recovery.

![Go](https://img.shields.io/badge/Go-1.23-00ADD8?style=flat&logo=go)
![Redis](https://img.shields.io/badge/Redis-7-DC382D?style=flat&logo=redis)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-316192?style=flat&logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker)
![License](https://img.shields.io/badge/license-MIT-green)

---

## What is SmartQueue?

SmartQueue is a background job processing infrastructure — the kind every production system needs but few engineers know how to build.

When a company sends 10,000 emails, processes payments, or generates PDF reports, it cannot block the user for 30 seconds. SmartQueue decouples the "acknowledge the user" step from the "do the heavy work" step:

```
WITHOUT SmartQueue:                 WITH SmartQueue:

user clicks "send campaign"         user clicks "send campaign"
server processes 10,000 emails      server responds in 50ms ✓
user waits 30 seconds               5 workers process emails in background
if server crashes → lost            if worker fails → automatic retry
if one email fails → unknown        if all retries fail → Dead Letter Queue
```

This is not a tutorial project. It is a system that mirrors what companies like Stripe, Shopify, and GitHub use to process millions of background jobs per day.

---

## Architecture

```
CLIENT (Postman / curl / frontend)
              |
              | POST /jobs
              v
    ┌─────────────────────┐
    │     API Layer        │  :8080
    │   handler.go         │  validates, routes, responds
    │   router.go          │  middleware: auth, logger, recovery, cors
    │   auth middleware    │  API Key validation on every request
    └─────────────────────┘
          |           |
          v           v
      [Redis]     [PostgreSQL]
      active       permanent
      queue        history
          |
          v
┌─────────────────────────────────────────┐
│            WORKER POOL                   │
│                                          │
│  worker-0  worker-1  worker-2            │
│  worker-3  worker-4                      │
│                                          │
│  5 goroutines running in parallel        │
│  each polling Redis every 500ms          │
└─────────────────────────────────────────┘
          |
    job processed?
    /             \
  YES              NO
   |                |
status=done      retries++
Postgres           |
updated        max retries reached?
                /           \
              NO              YES
               |               |
         re-enqueue          DLQ
         with backoff     status=failed
                          Postgres updated
```

---

## Why This Project Is Senior-Level

Most portfolio projects are CRUDs. SmartQueue solves **infrastructure problems**:

| Problem | Solution |
|---|---|
| Concurrent access to shared queue | `ZPopMin` atomic Redis operation |
| Two workers taking the same job | Atomic pop — one operation, indivisible |
| Job lost on server restart | Dual persistence: Redis + PostgreSQL |
| External service goes down | Exponential backoff with jitter |
| Job fails forever silently | Dead Letter Queue with error tracking |
| Thundering herd on retry | Random jitter breaks synchronized retries |
| Metrics missing under concurrency | `sync/atomic` — hardware-level thread safety |
| Server crash mid-job | Graceful shutdown with `WaitGroup` |
| Open API endpoints | API Key authentication with SHA256 hashing |

---

## Stack

| Technology | Role | Why |
|---|---|---|
| **Go 1.23** | Language | Goroutines: ~2KB each vs ~1MB OS threads |
| **Redis 7** | Active queue | Microsecond reads, `ZPopMin` atomicity |
| **PostgreSQL 15** | Persistence | ACID guarantees, historical queries |
| **Docker Compose** | Orchestration | One command to run everything |

---

## Project Structure

```
smartqueue/
├── cmd/
│   └── main.go                 entry point — wires all layers together
│                               connects Redis, PostgreSQL, starts workers
│                               handles SIGINT/SIGTERM gracefully
│
├── internal/
│   ├── auth/
│   │   ├── apikey.go           API Key struct, SHA256 hashing, generation
│   │   ├── store.go            PostgreSQL CRUD for keys (create, validate, revoke)
│   │   └── middleware.go       HTTP middleware — validates Bearer token on each request
│   │
│   ├── queue/
│   │   ├── job.go              Job struct — the unit of work
│   │   │                       Status: pending → processing → done/failed
│   │   ├── producer.go         Enqueue() → ZAdd to Redis Sorted Set
│   │   │                       preserves ID on retry (bug fixed in dev)
│   │   └── consumer.go         Dequeue() → ZPopMin (atomic pop)
│   │                           UpdateStatus() → updates job data in Redis
│   │
│   ├── worker/
│   │   ├── worker.go           infinite loop: dequeue → find handler → execute
│   │   │                       updates Redis + PostgreSQL on success/failure
│   │   │                       records metrics
│   │   └── pool.go             launches N goroutines with shared context
│   │                           WaitGroup ensures clean shutdown
│   │
│   ├── retry/
│   │   └── backoff.go          exponential backoff: delay = 2s * 2^attempt + jitter
│   │                           jitter prevents thundering herd
│   │                           after MaxRetries → sendToDLQ()
│   │                           updates PostgreSQL at every stage
│   │
│   ├── storage/
│   │   └── postgres.go         Save(), Update(), GetByID(), GetByStatus()
│   │                           indices on status, type, created_at
│   │                           JSONB payload for flexible job data
│   │
│   ├── metrics/
│   │   └── metrics.go          atomic int64 counters (no mutex needed)
│   │                           jobs_processed, jobs_failed, avg_ms, error_rate
│   │                           RWMutex only for duration slice
│   │
│   └── api/
│       ├── handler.go          one function per endpoint
│       │                       never returns null — always empty array
│       └── router.go           URL → handler mapping
│                               middleware stack: recovery → logger → cors → auth
│
├── Dockerfile                  multi-stage build: builder (300MB) → runner (15MB)
├── docker-compose.yml          app + redis + postgres with healthchecks
└── go.mod
```

---

## API Reference

All endpoints except `/health` and `POST /auth/keys` require:
```
Authorization: Bearer <your-api-key>
```

### Authentication

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/auth/keys` | Create a new API key (public) |
| `GET` | `/auth/keys` | List all keys |
| `DELETE` | `/auth/keys/:id` | Revoke a key |

### Jobs

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/jobs` | Enqueue a new job |
| `GET` | `/jobs` | List jobs by status |
| `GET` | `/jobs/:id` | Get job by ID |
| `GET` | `/metrics` | Real-time system metrics |
| `GET` | `/dlq` | List failed jobs |
| `POST` | `/dlq/:id/retry` | Manually retry a failed job |
| `GET` | `/health` | Health check (public) |

---

## How It Works — Key Concepts

### Priority Queue with Redis Sorted Sets

```
ZAdd queue:jobs score=1 job_id   ← priority 1 (urgent)
ZAdd queue:jobs score=2 job_id   ← priority 2 (normal)
ZAdd queue:jobs score=3 job_id   ← priority 3 (low)

ZPopMin queue:jobs               ← always takes the most urgent
                                    atomic: no two workers get the same job
```

### Exponential Backoff with Jitter

```
attempt 1 → wait 2s * 2¹ + random(0-1s) ≈ 4.5s
attempt 2 → wait 2s * 2² + random(0-1s) ≈ 8.7s
attempt 3 → wait 2s * 2³ + random(0-1s) ≈ 16.3s
attempt 4 → Dead Letter Queue

Without jitter: 1000 failed jobs retry simultaneously → cascade failure
With jitter:    retries spread across time → system recovers gracefully
```

### Dual Persistence Strategy

```
Redis  → source of truth for processing (fast, volatile)
Postgres → source of truth for history  (slow, permanent)

On job create:    Redis (enqueue) + Postgres (INSERT)
On job process:   Redis (status update) + Postgres (UPDATE)
On retry:         Redis (re-enqueue) + Postgres (UPDATE retries)
On DLQ:           Redis (status=failed) + Postgres (UPDATE status=failed)

If Redis restarts: queue lost, history in Postgres
If Postgres down:  processing continues, history gaps
```

### API Key Security

```
Key generated:  sk_live_ + 32 random bytes (base64) = 256-bit entropy
Stored in DB:   SHA256(key) — never the key itself
On request:     SHA256(incoming_key) → lookup in DB
                index on key_hash → microsecond validation

If DB is compromised: attacker gets hashes, not keys
                      SHA256 is one-way — keys are safe
```

---

## Bugs Found and Fixed in Development

### Bug 1 — Job Identity Lost on Retry

**Symptom:** Each retry created a new UUID. Three retries = three different job IDs. PostgreSQL could never be updated because the IDs didn't match.

**Root cause:** `producer.Enqueue()` called `uuid.NewString()` unconditionally on every call, including retries.

**Fix:**
```go
// BEFORE — always generates new ID
job.ID = uuid.NewString()

// AFTER — only generates ID if job is new
if job.ID == "" {
    job.ID = uuid.NewString()
    job.CreatedAt = time.Now()
}
```

### Bug 2 — Dead Letter Queue Always Empty

**Symptom:** Jobs failed and were sent to DLQ according to logs. `GET /dlq` returned `[]`. PostgreSQL showed `status=processing` forever.

**Root cause:** `Retryer` had no reference to `PostgresStore`. It updated Redis correctly but never touched Postgres. `sendToDLQ()` called `consumer.UpdateStatus()` (Redis only).

**Fix:** Added `store *storage.PostgresStore` to `Retryer`. Both `sendToDLQ()` and retry path now call `store.Update()`.

---

## Getting Started

### One-command startup (Docker)

```bash
git clone https://github.com/Darkblade1995/smartqueue
cd smartqueue
docker compose up --build -d
```

Verify:
```bash
curl http://localhost:8080/health
# {"status":"ok"}
```

### Create your first API key

```bash
curl -X POST http://localhost:8080/auth/keys \
  -H "Content-Type: application/json" \
  -d '{"name":"my-service"}'

# Save the "key" field — it is shown only once
```

### Enqueue a job

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <your-key>" \
  -d '{
    "type": "email",
    "priority": 1,
    "payload": {
      "to": "user@example.com",
      "subject": "Hello"
    }
  }'
```

### Check metrics

```bash
curl http://localhost:8080/metrics \
  -H "Authorization: Bearer <your-key>"

# {
#   "jobs_processed": 42,
#   "jobs_failed": 1,
#   "workers_active": 5,
#   "avg_processing_ms": 142.3,
#   "error_rate": 2.325...
# }
```

### Local development (without Docker)

Requirements: Go 1.23+, Redis, PostgreSQL

```bash
# Start dependencies
docker compose up postgres redis -d

# Run
REDIS_ADDR=localhost:6379 \
POSTGRES_URL=postgres://postgres:postgres@localhost:5433/smartqueue?sslmode=disable \
go run cmd/main.go
```

### Run tests

```bash
go test ./... -v
```

```
ok  smartqueue/internal/metrics   0.004s  (5 tests)
ok  smartqueue/internal/queue     0.005s  (4 tests)
ok  smartqueue/internal/retry     4.372s  (5 tests)
```

---

## Design Decisions

### Why ZPopMin instead of LPUSH/RPOP?

`LPUSH/RPOP` is FIFO but cannot handle priorities. `ZPopMin` on a Sorted Set gives us both atomicity and priority ordering in a single O(log N) operation. No locks needed.

### Why not an ORM?

`database/sql` with raw queries gives explicit control over what hits the database. ORMs generate inefficient queries and hide the SQL — unacceptable for a system where every query path is performance-sensitive.

### Why `sync/atomic` for metrics instead of `sync.Mutex`?

Mutexes serialize access. Under 5 concurrent workers all recording metrics simultaneously, a mutex creates contention. `atomic.AddInt64` maps to a single CPU instruction (`LOCK XADD`) — indivisible, no blocking.

### Why multi-stage Docker build?

The `golang:1.23-alpine` builder image is ~300MB. The final binary running on `alpine:3.19` is ~15MB. Multi-stage keeps the production image lean and free of build toolchain.

### Why SHA256 for API keys instead of bcrypt?

API keys are high-entropy random strings (256 bits), not human-chosen passwords. bcrypt's cost is designed to slow down dictionary attacks — unnecessary here. SHA256 is deterministic and fast, which is exactly what we need for per-request validation without latency overhead.

---

## Performance

```
Job enqueue (API response):      ~50-150ms
Worker processing (email mock):  ~100ms
Worker processing (payment mock): ~200ms
Worker processing (report mock):  ~500ms
Concurrent throughput (5 workers): 5x single-worker
Backoff timing:
  attempt 1 → ~4s
  attempt 2 → ~8s
  attempt 3 → ~16s
  → DLQ
```

---

## Author

**Luis Fernando Agamez Atehortúa**  
Software Engineer — Barranquilla, Colombia

- GitHub: [@Darkblade1995](https://github.com/Darkblade1995)
- LinkedIn: [luis-fernando-agamez](https://linkedin.com/in/luis-fernando-agamez)
- YouTube: [@programandoconlucho](https://youtube.com/@programandoconlucho)
- Portfolio: [newportafolioluisagamez.vercel.app](https://newportafolioluisagamez.vercel.app)

---

## License

MIT License — see [LICENSE](LICENSE) for details.

---

*Οὐδὲν ἀδύνατον τῷ φιλοπόνῳ.* — Ἀλέξανδρος ὁ Μέγας  
*"Nothing is impossible for the one who tries."* — Alexander the Great

---
