package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
	"github.com/valyala/fasthttp"
)

var (
	rdb *redis.Client
	db  *sql.DB
)

func init() {
	// 1. Redis 連線與清洗
	rawRedisURL := strings.TrimSpace(os.Getenv("REDIS_URL"))
	if rawRedisURL != "" {
		opt, err := redis.ParseURL(rawRedisURL)
		if err != nil {
			log.Printf("❌ Redis 解析失敗: [%s], 錯誤: %v", rawRedisURL, err)
		} else {
			rdb = redis.NewClient(opt)
			log.Println("✅ Redis 物件建立成功")
		}
	}

	// 2. PostgreSQL 連線與清洗
	rawDBURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if rawDBURL != "" {
		var err error
		db, err = sql.Open("postgres", rawDBURL)
		if err != nil {
			log.Printf("❌ DB 打開失敗: %v", err)
		} else {
			log.Println("✅ 資料庫物件建立成功")
		}
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	// 自動建表
	go func() {
		time.Sleep(3 * time.Second)
		if db != nil {
			_, err := db.Exec("CREATE TABLE IF NOT EXISTS system_logs (id SERIAL PRIMARY KEY, req_count BIGINT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
			if err != nil {
				log.Printf("⚠️ 建表失敗: %v", err)
			} else {
				log.Println("✅ 資料表 system_logs 已就緒")
			}
		}
	}()

	requestHandler := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set("Access-Control-Allow-Origin", "*")
		ctx.Response.Header.Set("Content-Type", "application/json")

		if string(ctx.Path()) == "/stats" {
			if db == nil {
				fmt.Fprintf(ctx, "{\"error\": \"DB Not Configured\"}")
				return
			}
			var count int64
			err := db.QueryRow("SELECT req_count FROM system_logs ORDER BY created_at DESC LIMIT 1").Scan(&count)
			if err != nil {
				fmt.Fprintf(ctx, "{\"error\": \"No Data\", \"msg\": \"%s\"}", err.Error())
			} else {
				fmt.Fprintf(ctx, "{\"last_sync_count\": %d}", count)
			}
			return
		}

		count := int64(0)
		if rdb != nil {
			count, _ = rdb.Incr(context.Background(), "total_requests").Result()
			// 每次點擊嘗試同步（加速測試）
			if db != nil && count % 5 == 0 {
				db.Exec("INSERT INTO system_logs (req_count) VALUES ($1)", count)
			}
		}
		fmt.Fprintf(ctx, "{\"status\":\"active\",\"current\":%d}", count)
	}

	port := os.Getenv("PORT")
	if port == "" { port = "8080" }
	log.Printf("🚀 戰神引擎 v4 啟動 | 端口: %s", port)
	fasthttp.ListenAndServe(":"+port, requestHandler)
}
