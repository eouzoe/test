package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
	"github.com/valyala/fasthttp"
)

var (
	rdb *redis.Client
	db  *sql.DB
	ctx_bg = context.Background()
)

func init() {
	// 清洗並連線 Redis
	redisURL := strings.TrimSpace(os.Getenv("REDIS_URL"))
	if opt, err := redis.ParseURL(redisURL); err == nil {
		rdb = redis.NewClient(opt)
	}
	// 清洗並連線 DB
	dbURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	db, _ = sql.Open("postgres", dbURL)
}

func main() {
	// 自動建表
	if db != nil {
		db.Exec("CREATE TABLE IF NOT EXISTS system_logs (id SERIAL PRIMARY KEY, req_count BIGINT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
	}

	requestHandler := func(ctx *fasthttp.RequestCtx) {
		path := string(ctx.Path())
		
		if path == "/stats" {
			var count int64
			err := db.QueryRow("SELECT req_count FROM system_logs ORDER BY id DESC LIMIT 1").Scan(&count)
			if err != nil {
				fmt.Fprintf(ctx, "{\"last_sync_count\": 0, \"msg\": \"no_data\"}")
			} else {
				fmt.Fprintf(ctx, "{\"last_sync_count\": %d}", count)
			}
			return
		}

		// 增加計數
		count, _ := rdb.Incr(ctx_bg, "war_engine_total").Result()
		
		// 強制同步到 DB (不設門檻)
		if db != nil {
			_, err := db.Exec("INSERT INTO system_logs (req_count) VALUES ($1)", count)
			if err != nil {
				log.Printf("❌ 同步失敗: %v", err)
			}
		}

		fmt.Fprintf(ctx, "{\"status\":\"active\",\"current\":%d}", count)
	}

	port := os.Getenv("PORT")
	if port == "" { port = "8080" }
	log.Printf("🚀 戰神引擎 v5 (強制同步版) 啟動")
	fasthttp.ListenAndServe(":"+port, requestHandler)
}
