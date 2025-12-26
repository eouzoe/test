package main

import (
    "context"
    "database/sql"
    "encoding/base64"
    "encoding/json"
    "fmt"
    "log/slog"
    "math"
    "os"
    "runtime"
    "strings"
    "time"

    "github.com/jackc/pgx/v5"
    "github.com/jackc/pgx/v5/stdlib"
    "github.com/redis/go-redis/v9"
    "github.com/valyala/fasthttp"
)

// Globals
var (
    rdb    *redis.Client
    db     *sql.DB
    ctx_bg = context.Background()

    // LogEnvelope carries a context.Context so trace IDs propagate to goroutines
    syncChan = make(chan LogEnvelope, 1000)

    // Token bucket Redis Lua (atomic) - stored also on disk as scripts/token_bucket.lua
    tokenBucketLua = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_per_sec = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local tokens_needed = tonumber(ARGV[4])

local state = redis.call("HMGET", key, "tokens", "ts")
local tokens = tonumber(state[1]) or capacity
local ts = tonumber(state[2]) or 0

if ts == 0 then
  ts = now
end

local elapsed = math.floor((now - ts) / 1000)
if elapsed > 0 then
  local refill = elapsed * refill_per_sec
  tokens = math.min(capacity, tokens + refill)
  ts = now
end

local allowed = 0
if tokens >= tokens_needed then
  tokens = tokens - tokens_needed
  allowed = 1
end

redis.call("HMSET", key, "tokens", tokens, "ts", ts)
redis.call("EXPIRE", key, math.ceil(capacity / math.max(refill_per_sec,1))*2)
return allowed
`
)

// LogEnvelope carries request-derived context and payload for batch processing
type LogEnvelope struct {
    Ctx       context.Context
    Count     int
    Received  time.Time
}

// HybridSearch placeholder types
type SearchResult struct {
    ID        string                 `json:"id"`
    Score     float64                `json:"score"`
    Meta      map[string]interface{} `json:"meta"`
}

type HybridSearch interface {
    Search(ctx context.Context, q string, topK int) ([]SearchResult, error)
}

// StartSpan/EndSpan are lightweight wrappers to allow future OpenTelemetry integration
func StartSpan(ctx context.Context, name string) context.Context {
    // Non-invasive: log span start with trace information if present in ctx
    slog.Debug("span.start", "name", name)
    return ctx
}

func EndSpan(ctx context.Context, name string, err error) {
    // Non-invasive: log span end
    if err != nil {
        slog.Warn("span.end.error", "name", name, "error", err)
    } else {
        slog.Debug("span.end", "name", name)
    }
}

func initLogger() {
    opts := &slog.HandlerOptions{Level: slog.LevelInfo}
    handler := slog.NewJSONHandler(os.Stdout, opts)
    logger := slog.New(handler)
    slog.SetDefault(logger)
}

// Parse JWT claims without verification (simulation for IDOR checks).
// Only use this for ownership checks where a full verification step is handled elsewhere.
func parseJWTUnsigned(token string) map[string]interface{} {
    parts := strings.Split(token, ".")
    if len(parts) < 2 {
        return nil
    }
    payload := parts[1]
    // base64 decode (URL encoding)
    b, err := base64.RawURLEncoding.DecodeString(payload)
    if err != nil {
        // try standard base64 fallback
        b, err = base64.StdEncoding.DecodeString(payload)
        if err != nil {
            return nil
        }
    }
    var claims map[string]interface{}
    if err := json.Unmarshal(b, &claims); err != nil {
        return nil
    }
    return claims
}

// CheckOwnership simulates JWT-based resource ownership check to prevent IDOR
func CheckOwnership(ctx *fasthttp.RequestCtx, resourceOwner string) bool {
    auth := string(ctx.Request.Header.Peek("Authorization"))
    if auth == "" {
        return false
    }
    if !strings.HasPrefix(auth, "Bearer ") {
        return false
    }
    token := strings.TrimPrefix(auth, "Bearer ")
    claims := parseJWTUnsigned(token)
    if claims == nil {
        return false
    }
    if sub, ok := claims["sub"].(string); ok {
        return sub == resourceOwner
    }
    return false
}

func initRedis() error {
    redisURL := os.Getenv("REDIS_URL")
    if redisURL == "" {
        redisURL = "redis://redis:6379/0"
    }
    opt, err := redis.ParseURL(redisURL)
    if err != nil {
        slog.Warn("Redis URL parse failed, using default", "error", err)
        opt = &redis.Options{Addr: "redis:6379"}
    }
    rdb = redis.NewClient(opt)

    // Test connection
    ctx, cancel := context.WithTimeout(ctx_bg, 3*time.Second)
    defer cancel()
    if err := rdb.Ping(ctx).Err(); err != nil {
        slog.Error("Redis ping failed", "error", err)
        return err
    }

    // Load token bucket script into Redis for EVALSHA use
    if _, err := rdb.ScriptLoad(ctx, tokenBucketLua).Result(); err != nil {
        slog.Warn("Failed to load token-bucket script", "error", err)
    }

    slog.Info("Redis initialized", "addr", opt.Addr)
    return nil
}

func checkHealth() error {
    ctx, cancel := context.WithTimeout(ctx_bg, 5*time.Second)
    defer cancel()
    if rdb != nil {
        if err := rdb.Ping(ctx).Err(); err != nil {
            slog.Warn("Redis health check failed (non-critical)", "error", err)
        } else {
            slog.Info("Redis healthy")
        }
    }
    if db != nil {
        if err := db.PingContext(ctx); err != nil {
            return fmt.Errorf("Database health check failed: %w", err)
        }
        slog.Info("DB healthy")
    } else {
        return fmt.Errorf("Database connection is nil")
    }
    return nil
}

// Token-bucket middleware using the token bucket Lua script loaded into Redis.
// capacity: tokens, refillRate: tokens per second, needed: tokens per request
func rateLimitMiddleware(ctx *fasthttp.RequestCtx) bool {
    start := StartSpan(ctx_bg, "rate_limit")
    defer EndSpan(start, "rate_limit", nil)

    ip := ctx.RemoteIP().String()
    key := "tb:" + ip

    // Parameters: capacity=50, refillRate=5 tokens/sec, need=1
    capacity := 50
    refill := 5
    need := 1
    now := time.Now().UnixMilli()

    res, err := rdb.Eval(ctx_bg, tokenBucketLua, []string{key}, capacity, refill, now, need).Result()
    if err != nil {
        slog.Warn("Token-bucket eval failed, fail-open", "error", err)
        return true
    }
    allowed, ok := res.(int64)
    if !ok {
        slog.Warn("Unexpected token-bucket response type", "value", res)
        return true
    }
    if allowed == 0 {
        ctx.SetStatusCode(fasthttp.StatusTooManyRequests)
        ctx.SetContentType("application/json")
        ctx.WriteString(`{"error":"rate_limit_exceeded"}`)
        return false
    }
    return true
}

// Batch worker: consumes LogEnvelope and writes batch to DB, with anomaly detection
func startBatchWorker() {
    slog.Info("Batch worker starting")
    baseline := 10

    batch := make([]LogEnvelope, 0, baseline)
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()

    flush := func() {
        if len(batch) == 0 {
            return
        }
        // Anomaly detection: if size > 3x baseline, flag
        if len(batch) > baseline*3 {
            slog.Warn("data_leak_suspected", "batch_size", len(batch))
        }

        // Build single INSERT with minimal allocations
        query := "INSERT INTO system_logs (req_count, created_at, embedding) VALUES "
        args := make([]interface{}, 0, len(batch)*3)
        vals := make([]string, 0, len(batch))
        for i, e := range batch {
            idx := i*3 + 1
            vals = append(vals, fmt.Sprintf("($%d,$%d,$%d)", idx, idx+1, idx+2))
            args = append(args, e.Count)
            args = append(args, e.Received)
            // reserve embedding as NULL for now (jsonb)
            args = append(args, nil)
        }
        query += strings.Join(vals, ",")

        spanCtx := StartSpan(ctx_bg, "db.batch_insert")
        _, err := db.Exec(query, args...)
        EndSpan(spanCtx, "db.batch_insert", err)
        if err != nil {
            slog.Error("Batch insert failed", "error", err, "batch_size", len(batch))
        } else {
            slog.Info("Batch insert succeeded", "batch_size", len(batch))
        }
        batch = batch[:0]
    }

    for {
        select {
        case e := <-syncChan:
            batch = append(batch, e)
            if len(batch) >= baseline {
                flush()
            }
        case <-ticker.C:
            flush()
        }
    }
}

func requestHandler(ctx *fasthttp.RequestCtx) {
    // Minimal allocations: use raw header access
    ctx.Response.Header.Set("Server", "SideShip")
    ctx.Response.Header.Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains; preload")
    ctx.Response.Header.Set("Content-Security-Policy", "default-src 'self'")
    ctx.Response.Header.Set("X-Frame-Options", "DENY")
    ctx.Response.Header.Set("X-Content-Type-Options", "nosniff")

    method := string(ctx.Method())
    path := string(ctx.Path())

    if method == "OPTIONS" {
        ctx.SetStatusCode(fasthttp.StatusNoContent)
        return
    }

    // API endpoints
    if strings.HasPrefix(path, "/api/") {
        if !rateLimitMiddleware(ctx) {
            return
        }

        // Example: IDOR protection for a resource path like /api/item/{owner}
        if strings.HasPrefix(path, "/api/item/") {
            // owner is last segment
            parts := strings.Split(path, "/")
            owner := parts[len(parts)-1]
            if !CheckOwnership(ctx, owner) {
                ctx.SetStatusCode(fasthttp.StatusForbidden)
                ctx.SetContentType("application/json")
                ctx.WriteString(`{"error":"forbidden","message":"ownership mismatch"}`)
                return
            }
        }

        handleAPI(ctx, path)
        return
    }

    // Root handler serves index.html or counter
    if path == "/" && method == "GET" {
        accept := string(ctx.Request.Header.Peek("Accept"))
        if strings.Contains(accept, "application/json") {
            if !rateLimitMiddleware(ctx) {
                return
            }
            handleCounter(ctx)
            return
        }
        if len(indexHTMLContent) > 0 {
            ctx.SetContentType("text/html; charset=utf-8")
            ctx.Write(indexHTMLContent)
            return
        }
        ctx.SetStatusCode(fasthttp.StatusNotFound)
        ctx.WriteString("Index page not found")
        return
    }

    ctx.SetStatusCode(fasthttp.StatusNotFound)
    ctx.WriteString("Not Found")
}

// handleCounter increments Redis counter and sends LogEnvelope to sync channel
func handleCounter(ctx *fasthttp.RequestCtx) {
    pipe := rdb.Pipeline()
    incr := pipe.Incr(ctx_bg, "war_engine_v6")
    if _, err := pipe.Exec(ctx_bg); err != nil {
        slog.Error("Redis increment failed", "error", err)
        ctx.SetStatusCode(fasthttp.StatusServiceUnavailable)
        ctx.SetContentType("application/json")
        ctx.WriteString(`{"error":"storage_unavailable"}`)
        return
    }
    count := int(incr.Val())

    // Build envelope with a request-derived context (attempt to propagate traceparent)
    reqCtx := ctx_bg
    if tp := string(ctx.Request.Header.Peek("traceparent")); tp != "" {
        reqCtx = context.WithValue(ctx_bg, "traceparent", tp)
    }

    env := LogEnvelope{Ctx: reqCtx, Count: count, Received: time.Now()}
    select {
    case syncChan <- env:
    default:
        slog.Warn("sync channel full, dropping envelope", "count", count)
    }

    ctx.SetContentType("application/json")
    fmt.Fprintf(ctx, `{"status":"active","current":%d}`, count)
}

func handleStats(ctx *fasthttp.RequestCtx) {
    var count int64
    err := db.QueryRow("SELECT req_count FROM system_logs ORDER BY id DESC LIMIT 1").Scan(&count)
    if err != nil {
        ctx.SetContentType("application/json")
        ctx.WriteString(`{"last_sync_count":0}`)
        return
    }
    ctx.SetContentType("application/json")
    fmt.Fprintf(ctx, `{"last_sync_count":%d}`, count)
}

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
        ctx.SetContentType("text/plain")
        fmt.Fprintf(ctx, "Sideship-Killer War-Engine v6\nStatus: Optimized\nRate-Limit: Redis-TokenBucket\nBatch-Sync: Enabled")
    default:
        ctx.SetStatusCode(fasthttp.StatusNotFound)
        ctx.WriteString(`{"error":"not_found"}`)
    }
}

// minimal index HTML holder (loaded at startup)
var indexHTMLContent []byte

func main() {
    // Force GOMAXPROCS to 1 to align with Docker 0.5 CPU quota and reduce context switches
    runtime.GOMAXPROCS(1)

    initLogger()
    slog.Info("Starting War Engine v6", "version", "6.0.0")

    if err := initRedis(); err != nil {
        slog.Warn("Redis init failed (app will continue, rate limit fail-open)", "error", err)
    }

    if err := initDatabase(); err != nil {
        slog.Error("DB init failed", "error", err)
        os.Exit(1)
    }

    if err := checkHealth(); err != nil {
        if strings.Contains(err.Error(), "Database") {
            slog.Error("Critical DB health failure", "error", err)
            os.Exit(1)
        }
        slog.Info("Startup health check completed")
    }

    // Ensure schema includes embedding column for RAG preparation
    _, err := db.Exec(`
        CREATE TABLE IF NOT EXISTS system_logs (
            id SERIAL PRIMARY KEY,
            req_count INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            embedding JSONB NULL
        )
    `)
    if err != nil {
        slog.Error("Failed to ensure system_logs table", "error", err)
        os.Exit(1)
    }
    slog.Info("Database table ready (embedding reserved)")

    // Load index.html if present
    for _, p := range []string{"./static/index.html", "./index.html"} {
        if b, err := os.ReadFile(p); err == nil {
            indexHTMLContent = b
            slog.Info("Index loaded", "path", p)
            break
        }
    }

    go startBatchWorker()

    handler := recoverMiddleware(requestHandler)

    port := os.Getenv("PORT")
    if port == "" {
        port = "8080"
    }
    addr := ":" + port
    slog.Info("Listening", "addr", addr)

    if err := fasthttp.ListenAndServe(addr, handler); err != nil {
        slog.Error("Server failed", "error", err)
        os.Exit(1)
    }
}

// recoverMiddleware kept minimal for zero-downtime
func recoverMiddleware(next fasthttp.RequestHandler) fasthttp.RequestHandler {
    return func(ctx *fasthttp.RequestCtx) {
        defer func() {
            if r := recover(); r != nil {
                slog.Error("panic recovered", "error", r, "path", string(ctx.Path()))
                ctx.SetStatusCode(fasthttp.StatusInternalServerError)
                ctx.SetContentType("application/json")
                ctx.WriteString(`{"error":"internal_server_error"}`)
            }
        }()
        next(ctx)
    }
}

// initDatabase unchanged logic but kept here for completeness
func initDatabase() error {
    dbURL := os.Getenv("DATABASE_URL")
    if dbURL == "" {
        dbURL = "postgres://user:pass@db:5432/mydb?sslmode=disable"
    }
    config, err := pgx.ParseConfig(dbURL)
    if err != nil {
        return fmt.Errorf("failed to parse database URL: %w", err)
    }
    connStr := stdlib.RegisterConnConfig(config)
    db, err = sql.Open("pgx", connStr)
    if err != nil {
        return fmt.Errorf("failed to open database: %w", err)
    }
    db.SetMaxOpenConns(25)
    db.SetMaxIdleConns(10)
    db.SetConnMaxLifetime(5 * time.Minute)
    ctx, cancel := context.WithTimeout(ctx_bg, 5*time.Second)
    defer cancel()
    if err := db.PingContext(ctx); err != nil {
        return fmt.Errorf("failed to ping database: %w", err)
    }
    return nil
}
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
	
	// Static file content (index.html loaded at startup)
	indexHTMLContent []byte
	
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
// Returns error only for critical failures (DB), Redis failures are logged as warnings
func checkHealth() error {
	ctx, cancel := context.WithTimeout(ctx_bg, 5*time.Second)
	defer cancel()
	
	// Check Redis (non-critical, log warning if unavailable)
	if rdb != nil {
		if err := rdb.Ping(ctx).Err(); err != nil {
			slog.Warn("Redis health check failed (non-critical)", "error", err)
			// Don't return error - Redis can be temporarily unavailable
		} else {
			slog.Info("Redis health check passed")
		}
	}
	
	// Check Database (critical - must be available)
	if db != nil {
		if err := db.PingContext(ctx); err != nil {
			return fmt.Errorf("Database health check failed: %w", err)
		}
		slog.Info("Database health check passed")
	} else {
		return fmt.Errorf("Database connection is nil")
	}
	
	return nil
}

// maskRedisURL masks password in Redis URL for logging
func maskRedisURL(url string) string {
	if url == "" {
		return "<empty>"
	}
	
	// Try to mask password in redis://user:pass@host format
	if strings.Contains(url, "@") {
		parts := strings.Split(url, "@")
		if len(parts) == 2 {
			authPart := parts[0]
			hostPart := parts[1]
			
			// Check if there's a password (format: redis://user:pass or redis://:pass)
			if strings.Contains(authPart, ":") {
				authParts := strings.Split(authPart, ":")
				if len(authParts) >= 3 {
					// Format: redis://user:pass
					schemeUser := strings.Join(authParts[:2], ":")
					masked := schemeUser + ":****@" + hostPart
					return masked
				} else if len(authParts) == 2 && authParts[1] != "" {
					// Format: redis://:pass
					masked := authParts[0] + ":****@" + hostPart
					return masked
				}
			}
		}
	}
	
	// If no password found or parsing failed, return as-is (it's probably safe)
	return url
}

// Initialize Redis Client (supports Podman gateway 10.89.0.1)
// Returns error but does not exit - allows app to continue with fail-open rate limiting
func initRedis() error {
	redisURL := os.Getenv("REDIS_URL")
	maskedURL := maskRedisURL(redisURL)
	
	if redisURL == "" {
		redisURL = "redis://redis:6379/0"
		maskedURL = "redis://redis:6379/0 (default)"
	}
	
	// Support Podman gateway format
	if !strings.HasPrefix(redisURL, "redis://") && !strings.HasPrefix(redisURL, "rediss://") {
		redisURL = "redis://" + redisURL
		maskedURL = "redis://" + maskedURL
	}
	
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		// Fallback to default configuration
		slog.Warn("Redis URL parse failed, using default", "error", err, "redis_url", maskedURL)
		opt = &redis.Options{
			Addr: "redis:6379",
		}
	}
	
	rdb = redis.NewClient(opt)
	
	// Test connection
	ctx, cancel := context.WithTimeout(ctx_bg, 5*time.Second)
	defer cancel()
	
	if err := rdb.Ping(ctx).Err(); err != nil {
		// Log detailed error but return it - don't exit
		slog.Error("Failed to connect to Redis (app will continue with fail-open rate limiting)", 
			"error", err, 
			"redis_url", maskedURL,
			"fallback", "rate limiting will allow all requests if Redis is unavailable")
		return fmt.Errorf("failed to connect to Redis: %w", err)
	}
	
	slog.Info("Redis initialized successfully", "addr", opt.Addr, "redis_url", maskedURL)
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
	
	// Stats endpoint (legacy compatibility)
	if path == "/stats" {
		handleStats(ctx)
		return
	}
	
	// Root path: serve index.html or counter API based on Accept header
	if path == "/" && method == "GET" {
		accept := string(ctx.Request.Header.Peek("Accept"))
		if strings.Contains(accept, "application/json") {
			// JSON request -> counter API
			if !rateLimitMiddleware(ctx) {
				return
			}
			handleCounter(ctx)
			return
		}
		// HTML request -> serve index.html
		if len(indexHTMLContent) > 0 {
			ctx.SetContentType("text/html; charset=utf-8")
			ctx.Write(indexHTMLContent)
		} else {
			ctx.SetStatusCode(fasthttp.StatusNotFound)
			ctx.WriteString("Index page not found")
		}
		return
	}
	
	// Serve index.html for /index.html path
	if path == "/index.html" {
		if len(indexHTMLContent) > 0 {
			ctx.SetContentType("text/html; charset=utf-8")
			ctx.Write(indexHTMLContent)
		} else {
			ctx.SetStatusCode(fasthttp.StatusNotFound)
			ctx.WriteString("Index page not found")
		}
		return
	}
	
	// 404 for all other paths (prevents file system exposure)
	ctx.SetStatusCode(fasthttp.StatusNotFound)
	ctx.WriteString("Not Found")
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
		// 讓 Cursor 把回傳內容改成這樣，增加「專業架構師」的 Vibe
		ctx.SetContentType("text/plain")
		fmt.Fprintf(ctx, "Sideship-Killer War-Engine v6\nStatus: Optimized\nRate-Limit: Redis-Lua-Active\nBatch-Sync: Enabled")
	default:
		ctx.SetStatusCode(fasthttp.StatusNotFound)
		ctx.WriteString(`{"error":"not_found","message":"API endpoint not found"}`)
	}
}

func main() {
	// Initialize JSON structured logger
	initLogger()
	
	slog.Info("Starting War Engine v6", "version", "6.0.0")
	
	// Initialize Redis (non-critical - app can continue without it)
	// rateLimitMiddleware has fail-open logic, so we don't exit on Redis failure
	if err := initRedis(); err != nil {
		slog.Error("Redis initialization failed (app will continue)", "error", err)
		// Don't exit - rate limiting will fail-open and allow all requests
	}
	
	// Initialize Database
	if err := initDatabase(); err != nil {
		slog.Error("Failed to initialize database", "error", err)
		os.Exit(1)
	}
	
	// Health check on startup (only DB is critical, Redis failures are logged but don't block startup)
	if err := checkHealth(); err != nil {
		// Only exit if DB is unavailable (critical)
		if strings.Contains(err.Error(), "Database") {
			slog.Error("Critical health check failed on startup (DB unavailable)", "error", err)
			os.Exit(1)
		}
		// Redis failures are non-critical, just log warning (already logged in checkHealth)
		slog.Info("Startup health check completed (Redis may be temporarily unavailable)")
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
	
	// Load index.html securely (avoid exposing project files)
	// Try multiple possible locations
	indexPaths := []string{
		"./static/index.html",
		"./index.html",
		"/root/index.html",
	}
	
	var indexHTML []byte
	var err error
	for _, path := range indexPaths {
		indexHTML, err = os.ReadFile(path)
		if err == nil {
			indexHTMLContent = indexHTML
			slog.Info("Index HTML loaded", "path", path)
			break
		}
	}
	
	if len(indexHTMLContent) == 0 {
		slog.Warn("Index HTML not found, root path will return 404", "attempted_paths", indexPaths)
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
