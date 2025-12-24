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
	// 1. 動態偵測 Redis (優先讀取雲端提供的變數)
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		// 本機開發 fallback
		rdb = redis.NewClient(&redis.Options{Addr: "localhost:6379", PoolSize: 1000})
	} else {
		opt, err := redis.ParseURL(redisURL)
		if err != nil {
			log.Printf("Redis URL Error: %v", err)
			rdb = redis.NewClient(&redis.Options{Addr: "localhost:6379"})
		} else {
			opt.PoolSize = 1000
			rdb = redis.NewClient(opt)
		}
	}

	// 2. 動態偵測 PostgreSQL
	pgConn := os.Getenv("DATABASE_URL")
	if pgConn == "" {
		pgConn = os.Getenv("POSTGRES_URL") // Zeabur 有時使用此變數
	}
	if pgConn == "" {
		pgConn = "host=localhost port=5432 user=postgres password=mysecretpassword dbname=postgres sslmode=disable"
	}
	
	var dbErr error
	db, dbErr = sql.Open("postgres", pgConn)
	if dbErr != nil {
		log.Printf("DB Connect Error: %v", dbErr)
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Write-Behind 非同步同步
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

	// 3. 重要：讀取雲端指定的 Port
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	s := &fasthttp.Server{
		Handler:     requestHandler,
		Concurrency: 256 * 1024,
	}

	log.Printf("🚀 雲端引擎啟動 | Port: %s", port)
	if err := s.ListenAndServe(":" + port); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
