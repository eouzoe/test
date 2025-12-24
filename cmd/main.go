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
	rdb        *redis.Client
	db         *sql.DB
	ctx_bg     = context.Background()
	syncChan   = make(chan int64, 1000) // 異步任務通道
)

func init() {
	// Redis 連線與清洗
	redisURL := strings.TrimSpace(os.Getenv("REDIS_URL"))
	if opt, err := redis.ParseURL(redisURL); err == nil {
		rdb = redis.NewClient(opt)
	}
	// DB 連線與清洗
	dbURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	db, _ = sql.Open("postgres", dbURL)
}

func main() {
	// 1. 自動建表
	if db != nil {
		db.Exec("CREATE TABLE IF NOT EXISTS system_logs (id SERIAL PRIMARY KEY, req_count BIGINT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
	}

	// 2. 啟動背景搬運工 (異步寫入 DB)
	go func() {
		log.Println("👷 背景搬運工已上線")
		for count := range syncChan {
			if db != nil {
				_, err := db.Exec("INSERT INTO system_logs (req_count) VALUES ($1)", count)
				if err != nil {
					log.Printf("❌ 寫入失敗: %v", err)
				}
			}
		}
	}()

	requestHandler := func(ctx *fasthttp.RequestCtx) {
		path := string(ctx.Path())

		// 統計路徑
		if path == "/stats" {
			var count int64
			err := db.QueryRow("SELECT req_count FROM system_logs ORDER BY id DESC LIMIT 1").Scan(&count)
			if err != nil {
				fmt.Fprintf(ctx, "{\"last_sync_count\": 0, \"msg\": \"syncing\"}")
			} else {
				fmt.Fprintf(ctx, "{\"last_sync_count\": %d}", count)
			}
			return
		}

		// 主路徑：增加計數
		count, err := rdb.Incr(ctx_bg, "war_engine_v6").Result()
		if err != nil {
			log.Printf("❌ Redis Incr 失敗: %v", err)
		}

		// 將最新計數丟入通道，不阻塞請求
		select {
		case syncChan <- count:
		default:
			// 如果通道滿了（每秒超過1000請求），暫時忽略同步以保證服務不當機
		}

		fmt.Fprintf(ctx, "{\"status\":\"active\",\"current\":%d}", count)
	}

	port := os.Getenv("PORT")
	if port == "" { port = "8080" }
	log.Printf("🚀 戰神引擎 v6 (異步實時版) 啟動")
	fasthttp.ListenAndServe(":"+port, requestHandler)
}
