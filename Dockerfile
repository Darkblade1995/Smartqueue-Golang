# ── Stage 1: Build ─────────────────────────────────────────
FROM golang:1.23-alpine AS builder

WORKDIR /app

# copiar dependencias primero (cache layer)
COPY go.mod go.sum ./
RUN go mod download

# copiar código fuente
COPY . .

# compilar binario estático
RUN CGO_ENABLED=0 GOOS=linux go build -o smartqueue ./cmd/main.go

# ── Stage 2: Run ───────────────────────────────────────────
FROM alpine:3.19

WORKDIR /app

# copiar solo el binario compilado
COPY --from=builder /app/smartqueue .

EXPOSE 8080

CMD ["./smartqueue"]