# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o catfish ./cmd

# Runtime stage
FROM alpine:3.20

WORKDIR /app

# create non-root user
RUN adduser -D -g '' catfish
# switch to it
USER catfish

COPY --from=builder /app/catfish .

EXPOSE 6432

ENTRYPOINT ["./catfish", "-config", "/etc/catfish/config.yml"]
