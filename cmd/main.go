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
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" { redisURL = "redis://localhost:6379" }
	opt, _ := redis.ParseURL(redisURL)
	rdb = redis.NewClient(opt)

	pgURL := os.Getenv("DATABASE_URL")
	if pgURL == "" { pgURL = os.Getenv("POSTGRES_URL") }
	if pgURL == "" { pgURL = "postgres://postgres:mysecretpassword@localhost:5432/postgres?sslmode=disable" }
	
	var err error
	db, err = sql.Open("postgres", pgURL)
	if err != nil { log.Printf("DB Open Error: %v", err) }
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	// 每 5 秒將 Redis 的總量存入 Postgres
	go func() {
		for {
			time.Sleep(5 * time.Second)
			if rdb != nil && db != nil {
				val, _ := rdb.Get(context.Background(), "total_requests").Int64()
				if val > 0 {
					db.Exec("INSERT INTO system_logs (req_count) VALUES ($1)", val)
				}
			}
		}
	}()

	requestHandler := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set("Access-Control-Allow-Origin", "*")
		ctx.Response.Header.Set("Content-Type", "application/json")

		switch string(ctx.Path()) {
		case "/stats":
			// 從資料庫讀取最後 5 筆紀錄
			var count int64
			var createdAt time.Time
			err := db.QueryRow("SELECT req_count, created_at FROM system_logs ORDER BY created_at DESC LIMIT 1").Scan(&count, &createdAt)
			
			if err != nil {
				fmt.Fprintf(ctx, "{\"error\": \"No data yet. Please wait 5 seconds or run hey test. Error: %v\"}", err)
			} else {
				fmt.Fprintf(ctx, "{\"last_sync_count\": %d, \"last_sync_time\": \"%s\"}", count, createdAt.Format("2006-01-02 15:04:05"))
			}

		default:
			// 增加計數
			rdb.Incr(context.Background(), "total_requests")
			fmt.Fprintf(ctx, "{\"status\":\"active\",\"msg\":\"Request recorded! Check /stats to see DB sync.\"}")
		}
	}

	port := os.Getenv("PORT")
	if port == "" { port = "8080" }
	log.Printf("🚀 戰神引擎升級版啟動 | 端口: %s", port)
	fasthttp.ListenAndServe(":"+port, requestHandler)
}
