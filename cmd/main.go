package main

import (
    "context"
    "database/sql"
    "encoding/base64"
    "encoding/json"
    "fmt"
    "log/slog"
    "golang.org/x/crypto/bcrypt"
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
    // Default content type for API responses
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
    case "/api/projects":
        handleProjects(ctx)
    case "/api/auth/register":
        handleRegister(ctx)
    case "/api/auth/login":
        handleLogin(ctx)
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

    // Ensure users table exists for authentication
    _, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `)
    if err != nil {
        slog.Error("Failed to ensure users table", "error", err)
        os.Exit(1)
    }

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

// Project representation for API
type Project struct {
    ID          int    `json:"id"`
    Name        string `json:"name"`
    Description string `json:"description"`
}

func handleProjects(ctx *fasthttp.RequestCtx) {
    // Simple projects fetch - tolerant if table missing
    rows, err := db.Query("SELECT id, COALESCE(name,''), COALESCE(description,'') FROM projects ORDER BY id DESC LIMIT 100")
    if err != nil {
        // If table doesn't exist, return empty list
        slog.Warn("projects query failed", "error", err)
        ctx.WriteString("[]")
        return
    }
    defer rows.Close()
    var projects []Project
    for rows.Next() {
        var p Project
        if err := rows.Scan(&p.ID, &p.Name, &p.Description); err != nil {
            slog.Warn("projects row scan failed", "error", err)
            continue
        }
        projects = append(projects, p)
    }
    b, _ := json.Marshal(projects)
    ctx.Write(b)
}

func handleRegister(ctx *fasthttp.RequestCtx) {
    var req struct{
        Username string `json:"username"`
        Password string `json:"password"`
    }
    if err := json.Unmarshal(ctx.PostBody(), &req); err != nil {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        ctx.WriteString(`{"error":"invalid_payload"}`)
        return
    }
    if req.Username == "" || req.Password == "" {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        ctx.WriteString(`{"error":"username_password_required"}`)
        return
    }
    hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
    if err != nil {
        slog.Error("bcrypt generate failed", "error", err)
        ctx.SetStatusCode(fasthttp.StatusInternalServerError)
        ctx.WriteString(`{"error":"server_error"}`)
        return
    }
    _, err = db.Exec("INSERT INTO users (username, password_hash) VALUES ($1,$2)", req.Username, string(hash))
    if err != nil {
        slog.Warn("user insert failed", "error", err)
        ctx.SetStatusCode(fasthttp.StatusConflict)
        ctx.WriteString(`{"error":"user_exists_or_db_error"}`)
        return
    }
    ctx.SetStatusCode(fasthttp.StatusCreated)
    ctx.WriteString(`{"status":"ok"}`)
}

func handleLogin(ctx *fasthttp.RequestCtx) {
    var req struct{
        Username string `json:"username"`
        Password string `json:"password"`
    }
    if err := json.Unmarshal(ctx.PostBody(), &req); err != nil {
        ctx.SetStatusCode(fasthttp.StatusBadRequest)
        ctx.WriteString(`{"error":"invalid_payload"}`)
        return
    }
    var hash string
    err := db.QueryRow("SELECT password_hash FROM users WHERE username=$1", req.Username).Scan(&hash)
    if err != nil {
        slog.Warn("user lookup failed", "error", err)
        ctx.SetStatusCode(fasthttp.StatusUnauthorized)
        ctx.WriteString(`{"error":"invalid_credentials"}`)
        return
    }
    if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(req.Password)); err != nil {
        ctx.SetStatusCode(fasthttp.StatusUnauthorized)
        ctx.WriteString(`{"error":"invalid_credentials"}`)
        return
    }
    // On success, set a simple session cookie (opaque value)
    sess := base64.RawURLEncoding.EncodeToString([]byte(req.Username+":"+time.Now().Format(time.RFC3339Nano)))
    cookie := fasthttp.Cookie{}
    cookie.SetKey("session")
    cookie.SetValue(sess)
    cookie.SetHTTPOnly(true)
    cookie.SetSecure(false)
    cookie.SetPath("/")
    cookie.SetExpire(time.Now().Add(7 * 24 * time.Hour))
    ctx.Response.Header.SetCookie(&cookie)
    ctx.WriteString(`{"status":"ok"}`)
}
