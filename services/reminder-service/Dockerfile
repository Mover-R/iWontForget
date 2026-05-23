FROM golang:1.24.13-alpine AS builder

WORKDIR /app

COPY pkg/ ./pkg/
COPY services/reminder-service/ ./services/reminder-service/

WORKDIR /app/services/reminder-service

RUN GOWORK=off go mod download

RUN CGO_ENABLED=0 GOOS=linux GOWORK=off go build -o /main ./cmd/main.go

FROM alpine:latest

WORKDIR /app

COPY --from=builder /main .
COPY services/reminder-service/config/ ./config/

EXPOSE 8081

CMD ["sh", "-c", "./main -config ./config/${APP_ENV}.yaml"]
