package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
	"github.com/valyala/fasthttp"
)

// Global Configuration
var (
	rdb      *redis.Client
	db       *sql.DB
	ctx_bg   = context.Background()
	syncChan = make(chan int, 1000) // Buffered channel for batch processing
	
	// Static file server
	fs *fasthttp.FS
	
	// Redis Lua Script for Rate Limiting (Atomic check-and-set)
	rateLimitLuaScript = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local current = redis.call("INCR", key)
if current == 1 then
    redis.call("EXPIRE", key, window)
end
if current > limit then
    return 0
end
return 1
`
)

// Initialize Global Structured Logger (JSON format)
func initLogger() {
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	handler := slog.NewJSONHandler(os.Stdout, opts)
	logger := slog.New(handler)
	slog.SetDefault(logger)
}

// CheckHealth verifies Redis and DB connections
func checkHealth() error {
	// Check Redis
	ctx, cancel := context.WithTimeout(ctx_bg, 5*time.Second)
	defer cancel()
	
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("Redis health check failed: %w", err)
	}
	slog.Info("Redis health check passed")
	
	// Check Database
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("Database health check failed: %w", err)
	}
	slog.Info("Database health check passed")
	
	return nil
}

// Initialize Redis Client (supports Podman gateway 10.89.0.1)
func initRedis() error {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://redis:6379/0"
	}
	
	// Support Podman gateway format
	if !strings.HasPrefix(redisURL, "redis://") && !strings.HasPrefix(redisURL, "rediss://") {
		redisURL = "redis://" + redisURL
	}
	
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		// Fallback to default configuration
		slog.Warn("Redis URL parse failed, using default", "error", err)
		opt = &redis.Options{
			Addr: "redis:6379",
		}
	}
	
	rdb = redis.NewClient(opt)
	
	// Test connection
	ctx, cancel := context.WithTimeout(ctx_bg, 5*time.Second)
	defer cancel()
	
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis: %w", err)
	}
	
	slog.Info("Redis initialized successfully", "addr", opt.Addr)
	return nil
}

// Initialize PostgreSQL Connection using pgx/v5 (supports Podman gateway 10.89.0.1)
func initDatabase() error {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://user:pass@db:5432/mydb?sslmode=disable"
	}
	
	// Parse connection string with pgx
	config, err := pgx.ParseConfig(dbURL)
	if err != nil {
		return fmt.Errorf("failed to parse database URL: %w", err)
	}
	
	// Register pgx driver
	connStr := stdlib.RegisterConnConfig(config)
	
	// Open connection using stdlib (database/sql compatible)
	db, err = sql.Open("pgx", connStr)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}
	
	// Set connection pool settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)
	
	// Test connection
	ctx, cancel := context.WithTimeout(ctx_bg, 5*time.Second)
	defer cancel()
	
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}
	
	slog.Info("Database initialized successfully")
	return nil
}

// Recover Middleware - Zero-Downtime Resilience
func recoverMiddleware(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("Panic recovered", "error", r, "path", string(ctx.Path()))
				ctx.SetStatusCode(fasthttp.StatusInternalServerError)
				ctx.SetContentType("application/json")
				ctx.WriteString(`{"error":"internal_server_error","message":"服務暫時無法處理請求"}`)
			}
		}()
		next(ctx)
	}
}

// Distributed Rate Limiter using Redis Lua Script (Atomic check-and-set)
func rateLimitMiddleware(ctx *fasthttp.RequestCtx) bool {
	userIP := ctx.RemoteIP().String()
	key := "rate_limit:" + userIP
	
	// Rate limit: 20 requests per 10 seconds
	limit := 20
	window := 10
	
	result, err := rdb.Eval(ctx_bg, rateLimitLuaScript, []string{key}, limit, window).Result()
	if err != nil {
		slog.Warn("Rate limit check failed", "error", err)
		// On Redis error, allow request (fail-open for availability)
		return true
	}
	
	if result.(int64) == 0 {
		ctx.SetStatusCode(fasthttp.StatusTooManyRequests)
		ctx.SetContentType("application/json")
		ctx.WriteString(`{"error":"rate_limit_exceeded","message":"請求過於頻繁"}`)
		return false
	}
	
	return true
}

// Batch Persistence Worker
func startBatchWorker() {
	slog.Info("Batch persistence worker started")
	
	batch := make([]int, 0, 10) // Buffer size: 10 items
	ticker := time.NewTicker(5 * time.Second) // Flush interval: 5 seconds
	defer ticker.Stop()
	
	flushBatch := func() {
		if len(batch) == 0 {
			return
		}
		
		// Build batch insert SQL
		query := "INSERT INTO system_logs (req_count) VALUES "
		values := make([]string, len(batch))
		args := make([]interface{}, len(batch))
		
		for i, count := range batch {
			values[i] = fmt.Sprintf("($%d)", i+1)
			args[i] = count
		}
		
		query += strings.Join(values, ", ")
		
		// Execute batch insert
		start := time.Now()
		_, err := db.Exec(query, args...)
		duration := time.Since(start)
		
		if err != nil {
			slog.Error("Batch insert failed", "error", err, "batch_size", len(batch), "duration_ms", duration.Milliseconds())
		} else {
			slog.Info("Batch insert successful", "batch_size", len(batch), "duration_ms", duration.Milliseconds())
		}
		
		// Clear batch
		batch = batch[:0]
	}
	
	for {
		select {
		case count := <-syncChan:
			batch = append(batch, count)
			// Flush when buffer reaches 10 items
			if len(batch) >= 10 {
				flushBatch()
			}
		case <-ticker.C:
			// Flush every 5 seconds
			flushBatch()
		}
	}
}

// Main Request Handler
func requestHandler(ctx *fasthttp.RequestCtx) {
	path := string(ctx.Path())
	method := string(ctx.Method())
	
	// Hide Server header to prevent version leaking
	ctx.Response.Header.Set("Server", "SideShip")
	
	// Security headers
	ctx.Response.Header.Set("Content-Security-Policy", "default-src 'self'")
	ctx.Response.Header.Set("X-Frame-Options", "DENY")
	ctx.Response.Header.Set("X-Content-Type-Options", "nosniff")
	
	// CORS headers
	ctx.Response.Header.Set("Access-Control-Allow-Origin", "*")
	ctx.Response.Header.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	ctx.Response.Header.Set("Access-Control-Allow-Headers", "Content-Type")
	
	// Handle OPTIONS requests
	if method == "OPTIONS" {
		ctx.SetStatusCode(fasthttp.StatusNoContent)
		return
	}
	
	// API routes
	if strings.HasPrefix(path, "/api/") {
		// Apply rate limiting to API endpoints
		if !rateLimitMiddleware(ctx) {
			return // Rate limited
		}
		
		handleAPI(ctx, path)
		return
	}
	
	// Counter endpoint (legacy compatibility)
	if path == "/" && method == "GET" {
		accept := string(ctx.Request.Header.Peek("Accept"))
		if strings.Contains(accept, "application/json") {
			if !rateLimitMiddleware(ctx) {
				return
			}
			handleCounter(ctx)
			return
		}
	}
	
	// Stats endpoint (legacy compatibility)
	if path == "/stats" {
		handleStats(ctx)
		return
	}
	
	// Static file server
	fsHandler := fs.NewRequestHandler()
	fsHandler(ctx)
}

// Handle Counter API
func handleCounter(ctx *fasthttp.RequestCtx) {
	// Increment counter in Redis
	pipe := rdb.Pipeline()
	incr := pipe.Incr(ctx_bg, "war_engine_v6")
	_, err := pipe.Exec(ctx_bg)
	
	if err != nil {
		slog.Error("Redis increment failed", "error", err)
		ctx.SetStatusCode(fasthttp.StatusServiceUnavailable)
		ctx.SetContentType("application/json")
		ctx.WriteString(`{"error":"storage_unavailable","message":"服務暫時無法處理請求"}`)
		return
	}
	
	count := incr.Val()
	
	// Send to batch worker (non-blocking)
	select {
	case syncChan <- int(count):
		// Successfully queued
	default:
		// Channel full, log warning but continue
		slog.Warn("Sync channel full, dropping count", "count", count)
	}
	
	ctx.SetContentType("application/json")
	fmt.Fprintf(ctx, `{"status":"active","current":%d}`, count)
}

// Handle Stats API
func handleStats(ctx *fasthttp.RequestCtx) {
	var count int64
	err := db.QueryRow("SELECT req_count FROM system_logs ORDER BY id DESC LIMIT 1").Scan(&count)
	if err != nil {
		ctx.SetContentType("application/json")
		ctx.WriteString(`{"last_sync_count":0,"msg":"syncing"}`)
		return
	}
	
	ctx.SetContentType("application/json")
	fmt.Fprintf(ctx, `{"last_sync_count":%d}`, count)
}

// Handle API Routes
func handleAPI(ctx *fasthttp.RequestCtx, path string) {
	ctx.SetContentType("application/json")
	
	switch path {
	case "/api/counter":
		handleCounter(ctx)
	case "/api/stats":
		handleStats(ctx)
	case "/api/health":
		if err := checkHealth(); err != nil {
			ctx.SetStatusCode(fasthttp.StatusServiceUnavailable)
			ctx.WriteString(fmt.Sprintf(`{"status":"unhealthy","error":"%s"}`, err.Error()))
			return
		}
		ctx.WriteString(`{"status":"healthy","service":"sideship-killer"}`)
	default:
		ctx.SetStatusCode(fasthttp.StatusNotFound)
		ctx.WriteString(`{"error":"not_found","message":"API endpoint not found"}`)
	}
}

func main() {
	// Initialize JSON structured logger
	initLogger()
	
	slog.Info("Starting War Engine v6", "version", "6.0.0")
	
	// Initialize Redis
	if err := initRedis(); err != nil {
		slog.Error("Failed to initialize Redis", "error", err)
		os.Exit(1)
	}
	
	// Initialize Database
	if err := initDatabase(); err != nil {
		slog.Error("Failed to initialize database", "error", err)
		os.Exit(1)
	}
	
	// Health check on startup
	if err := checkHealth(); err != nil {
		slog.Error("Health check failed on startup", "error", err)
		os.Exit(1)
	}
	
	// Create table if not exists
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS system_logs (
			id SERIAL PRIMARY KEY,
			req_count INTEGER NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		slog.Error("Failed to create table", "error", err)
		os.Exit(1)
	}
	slog.Info("Database table ready")
	
	// Initialize static file server
	workDir, _ := os.Getwd()
	fs = &fasthttp.FS{
		Root:               workDir,
		IndexNames:         []string{"index.html"},
		GenerateIndexPages: false,
		Compress:           true,
		AcceptByteRange:    true,
	}
	
	// Start batch persistence worker
	go startBatchWorker()
	
	// Wrap handler with recover middleware
	handler := recoverMiddleware(requestHandler)
	
	// Get port from environment
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	
	addr := ":" + port
	slog.Info("War Engine v6 listening", "addr", addr, "features", "rate_limit,batch_persistence,recovery")
	
	// Start server
	if err := fasthttp.ListenAndServe(addr, handler); err != nil {
		slog.Error("Server failed", "error", err)
		os.Exit(1)
	}
}
