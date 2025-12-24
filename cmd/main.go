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
	// 1. Redis 連線 (加入安全檢查)
	redisURL := os.Getenv("REDIS_URL")
	if redisURL != "" {
		opt, err := redis.ParseURL(redisURL)
		if err != nil {
			log.Printf("⚠️ Redis URL 格式錯誤: %v", err)
		} else {
			rdb = redis.NewClient(opt)
		}
	} else {
		log.Println("⚠️ 找不到 REDIS_URL，Redis 功能將停用")
	}

	// 2. PostgreSQL 連線
	pgURL := os.Getenv("DATABASE_URL")
	if pgURL != "" {
		var err error
		db, err = sql.Open("postgres", pgURL)
		if err != nil {
			log.Printf("❌ 資料庫打開失敗: %v", err)
		}
	} else {
		log.Println("⚠️ 找不到 DATABASE_URL，資料庫功能將停用")
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	// 自動建表 (增加 db 檢查)
	go func() {
		time.Sleep(5 * time.Second)
		if db != nil {
			_, err := db.Exec("CREATE TABLE IF NOT EXISTS system_logs (id SERIAL PRIMARY KEY, req_count BIGINT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
			if err != nil { log.Printf("⚠️ 建表失敗: %v", err) }
		}
	}()

	// 資料同步 (增加 rdb 與 db 檢查)
	go func() {
		for {
			time.Sleep(5 * time.Second)
			if rdb != nil && db != nil {
				val, err := rdb.Get(context.Background(), "total_requests").Int64()
				if err == nil && val > 0 {
					_, err := db.Exec("INSERT INTO system_logs (req_count) VALUES ($1)", val)
					if err != nil { log.Printf("⚠️ 寫入失敗: %v", err) }
				}
			}
		}
	}()

	requestHandler := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set("Access-Control-Allow-Origin", "*")
		ctx.Response.Header.Set("Content-Type", "application/json")

		switch string(ctx.Path()) {
		case "/stats":
			if db == nil {
				fmt.Fprintf(ctx, "{\"error\": \"Database not configured\"}")
				return
			}
			var count int64
			var createdAt time.Time
			err := db.QueryRow("SELECT req_count, created_at FROM system_logs ORDER BY created_at DESC LIMIT 1").Scan(&count, &createdAt)
			if err != nil {
				fmt.Fprintf(ctx, "{\"error\": \"Query failed\", \"detail\": \"%s\"}", err.Error())
			} else {
				fmt.Fprintf(ctx, "{\"last_sync_count\": %d, \"last_sync_time\": \"%s\"}", count, createdAt.Format("2006-01-02 15:04:05"))
			}
		default:
			count := int64(0)
			if rdb != nil {
				count, _ = rdb.Incr(context.Background(), "total_requests").Result()
			}
			fmt.Fprintf(ctx, "{\"status\":\"active\",\"current_session_count\":%d,\"goroutines\":%d}", count, runtime.NumGoroutine())
		}
	}

	port := os.Getenv("PORT")
	if port == "" { port = "8080" }
	log.Printf("🚀 戰神引擎安全版啟動 | 端口: %s", port)
	fasthttp.ListenAndServe(":"+port, requestHandler)
}
