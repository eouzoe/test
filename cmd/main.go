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
)

func init() {
	// 1. Redis 連線 (支援不同平台的變數名)
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" { redisURL = "redis://localhost:6379" }
	opt, _ := redis.ParseURL(redisURL)
	rdb = redis.NewClient(opt)

	// 2. PostgreSQL 連線 (暴力偵測)
	pgURL := os.Getenv("DATABASE_URL")
	if pgURL == "" { pgURL = os.Getenv("POSTGRES_URL") }
	
	// 如果還是空值，嘗試手動組合 (適用於某些環境提供分散變數)
	if pgURL == "" {
		host := os.Getenv("POSTGRES_HOST")
		port := os.Getenv("POSTGRES_PORT")
		user := os.Getenv("POSTGRES_USER")
		pass := os.Getenv("POSTGRES_PASSWORD")
		dbname := os.Getenv("POSTGRES_DB")
		if host != "" {
			pgURL = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, pass, host, port, dbname)
		}
	}

	// 最終預設
	if pgURL == "" {
		pgURL = "postgres://postgres:mysecretpassword@localhost:5432/postgres?sslmode=disable"
	}
	
	log.Printf("📡 嘗試連線資料庫...")
	var err error
	db, err = sql.Open("postgres", pgURL)
	if err != nil {
		log.Printf("❌ 資料庫連線失敗: %v", err)
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	// 自動建表
	go func() {
		time.Sleep(3 * time.Second)
		if db != nil {
			_, err := db.Exec("CREATE TABLE IF NOT EXISTS system_logs (id SERIAL PRIMARY KEY, req_count BIGINT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
			if err != nil { log.Printf("⚠️ 自動建表失敗: %v", err) }
		}
	}()

	// 資料同步
	go func() {
		for {
			time.Sleep(5 * time.Second)
			if rdb != nil && db != nil {
				val, _ := rdb.Get(context.Background(), "total_requests").Int64()
				if val > 0 {
					_, err := db.Exec("INSERT INTO system_logs (req_count) VALUES ($1)", val)
					if err != nil { log.Printf("⚠️ 同步數據失敗: %v", err) }
				}
			}
		}
	}()

	requestHandler := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set("Access-Control-Allow-Origin", "*")
		ctx.Response.Header.Set("Content-Type", "application/json")

		switch string(ctx.Path()) {
		case "/stats":
			var count int64
			var createdAt time.Time
			err := db.QueryRow("SELECT req_count, created_at FROM system_logs ORDER BY created_at DESC LIMIT 1").Scan(&count, &createdAt)
			if err != nil {
				fmt.Fprintf(ctx, "{\"error\": \"DB query failed. Please check Logs.\", \"debug\": \"%s\"}", err.Error())
			} else {
				fmt.Fprintf(ctx, "{\"last_sync_count\": %d, \"last_sync_time\": \"%s\"}", count, createdAt.Format("2006-01-02 15:04:05"))
			}
		default:
			rdb.Incr(context.Background(), "total_requests")
			fmt.Fprintf(ctx, "{\"status\":\"industrial_active\",\"goroutines\":%d}", runtime.NumGoroutine())
		}
	}

	port := os.Getenv("PORT")
	if port == "" { port = "8080" }
	fasthttp.ListenAndServe(":"+port, requestHandler)
}
