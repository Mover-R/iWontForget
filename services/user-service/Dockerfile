FROM golang:1.23.6-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /main ./cmd/main.go

FROM alpine:latest

WORKDIR /app

# Копируем бинарник и конфиг
COPY --from=builder /main .
COPY ./config/config.yaml ./config/

EXPOSE 8081

CMD ["./main"]