package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
	"github.com/valyala/fasthttp"
)

var (
	rdb    *redis.Client
	db     *sql.DB
	events = make(chan struct{}, 100000)
)

func init() {
	// Redis 連線優化
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379"
	}
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Printf("Redis URL error, using default: %v", err)
		rdb = redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	} else {
		rdb = redis.NewClient(opt)
	}

	// DB 連線優化
	pgURL := os.Getenv("DATABASE_URL")
	if pgURL == "" {
		pgURL = os.Getenv("POSTGRES_URL")
	}
	if pgURL == "" {
		pgURL = "postgres://postgres:mysecretpassword@localhost:5432/postgres?sslmode=disable"
	}
	
	// 注意：Zeabur 的 DATABASE_URL 通常已經是完整 URL 格式
	db, err = sql.Open("postgres", pgURL)
	if err != nil {
		log.Printf("DB Open Error: %v", err)
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Write-Behind 資料同步
	go func() {
		for {
			time.Sleep(5 * time.Second)
			if rdb != nil && db != nil {
				val, _ := rdb.Get(context.Background(), "total_requests").Int64()
				if val > 0 {
					_, _ = db.Exec("INSERT INTO system_logs (req_count) VALUES ($1)", val)
				}
			}
		}
	}()

	requestHandler := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set("Access-Control-Allow-Origin", "*")
		ctx.Response.Header.Set("Content-Type", "application/json")
		select {
		case events <- struct{}{}:
			go rdb.Incr(context.Background(), "total_requests")
		default:
		}
		fmt.Fprintf(ctx, "{\"status\":\"industrial_active\",\"goroutines\":%d}", runtime.NumGoroutine())
	}

	port := os.Getenv("PORT")
	if port == "" { port = "8080" }

	log.Printf("🚀 戰神引擎雲端版啟動 | 端口: %s", port)
	if err := fasthttp.ListenAndServe(":"+port, requestHandler); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
