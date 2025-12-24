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
	// 1. 自動偵測 Redis URL
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379"
	}
	
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Printf("Redis URL 格式錯誤: %v，嘗試本地連線", err)
		rdb = redis.NewClient(&redis.Options{Addr: "localhost:6379", PoolSize: 1000})
	} else {
		opt.PoolSize = 1000
		rdb = redis.NewClient(opt)
	}

	// 2. 自動偵測 PostgreSQL
	pgConn := os.Getenv("DATABASE_URL")
	if pgConn == "" {
		pgConn = os.Getenv("POSTGRES_URL")
	}
	if pgConn == "" {
		pgConn = "host=localhost port=5432 user=postgres password=mysecretpassword dbname=postgres sslmode=disable"
	}
	
	var dbErr error
	db, dbErr = sql.Open("postgres", pgConn)
	if dbErr != nil {
		log.Printf("資料庫連線失敗: %v", dbErr)
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Write-Behind: 每 5 秒將 Redis 數據同步回 Postgres
	go func() {
		for {
			time.Sleep(5 * time.Second)
			if rdb == nil || db == nil { continue }
			val, err := rdb.Get(context.Background(), "total_requests").Int64()
			if err == nil && val > 0 {
				_, _ = db.Exec("INSERT INTO system_logs (req_count) VALUES ($1)", val)
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

	// 3. 讀取 Zeabur 分配的 Port
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	s := &fasthttp.Server{
		Handler:     requestHandler,
		Concurrency: 256 * 1024,
	}

	log.Printf("🚀 戰神引擎雲端版啟動 | 端口: %s", port)
	if err := s.ListenAndServe(":" + port); err != nil {
		log.Fatalf("啟動失敗: %v", err)
	}
}
