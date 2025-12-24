package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
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
	rdb = redis.NewClient(&redis.Options{Addr: "localhost:6379", PoolSize: 1000})
	connStr := "host=localhost port=5432 user=postgres password=mysecretpassword dbname=postgres sslmode=disable"
	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Printf("DB Connect Error: %v", err)
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	// 工業化數據同步：Write-Behind
	go func() {
		for {
			time.Sleep(5 * time.Second)
			val, _ := rdb.Get(context.Background(), "total_requests").Int64()
			if val > 0 {
				db.Exec("INSERT INTO system_logs (req_count) VALUES ($1)", val)
			}
		}
	}()

	requestHandler := func(ctx *fasthttp.RequestCtx) {
		// 重要：讓前端能讀取數據
		ctx.Response.Header.Set("Access-Control-Allow-Origin", "*")
		ctx.Response.Header.Set("Content-Type", "application/json")
		
		// 非同步寫入任務
		select {
		case events <- struct{}{}:
			go rdb.Incr(context.Background(), "total_requests")
		default:
		}

		fmt.Fprintf(ctx, `{"status":"industrial_active","goroutines":%d}`, runtime.NumGoroutine())
	}

	// 使用最穩定的 Server 配置
	s := &fasthttp.Server{
		Handler:     requestHandler,
		Concurrency: 256 * 1024,
		ReadTimeout: 5 * time.Second,
	}

	log.Printf("🚀 戰神引擎已啟動 | 監聽 :8080")
	if err := s.ListenAndServe(":8080"); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
