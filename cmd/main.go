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
	// 優先讀取雲端 Redis URL，否則連線本地
	redisAddr := os.Getenv("REDIS_URL")
	if redisAddr == "" {
		redisAddr = "redis://localhost:6379"
	}

	opt, err := redis.ParseURL(redisAddr)
	if err != nil {
		log.Printf("Redis URL Parse Error: %v, using localhost default", err)
		rdb = redis.NewClient(&redis.Options{Addr: "localhost:6379", PoolSize: 1000})
	} else {
		opt.PoolSize = 1000
		rdb = redis.NewClient(opt)
	}

	// 優先讀取雲端 DB URL，否則連線本地
	pgConn := os.Getenv("DATABASE_URL")
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

	// Write-Behind 資料非同步寫入 Postgres
	go func() {
		for {
			time.Sleep(5 * time.Second)
			val, _ := rdb.Get(context.Background(), "total_requests").Int64()
			if val > 0 && db != nil {
				_, err := db.Exec("INSERT INTO system_logs (req_count) VALUES ($1)", val)
				if err != nil {
					log.Printf("SQL Insert Error: %v", err)
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

		fmt.Fprintf(ctx, `{"status":"industrial_active","goroutines":%d}`, runtime.NumGoroutine())
	}

	s := &fasthttp.Server{
		Handler:     requestHandler,
		Concurrency: 256 * 1024,
		ReadTimeout: 5 * time.Second,
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("🚀 戰神引擎已啟動 | 監聽 :%s", port)
	if err := s.ListenAndServe(":" + port); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
