FROM golang:1.24-alpine AS builder
WORKDIR /app
# 修正編譯快取權限問題
ENV GOCACHE=/root/.cache/go-build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o main ./cmd/main.go

FROM alpine:latest
WORKDIR /root/
# 安裝基本憑證以便連線雲端資料庫
RUN apk --no-cache add ca-certificates
COPY --from=builder /app/main .
EXPOSE 8080
CMD ["./main"]
