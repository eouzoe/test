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
    "net/url"
    "time"
    "strconv"
    "sync"
    xrate "golang.org/x/time/rate"

    "github.com/jackc/pgx/v5"
    "github.com/jackc/pgx/v5/stdlib"
    "github.com/redis/go-redis/v9"
    lru "github.com/hashicorp/golang-lru"
    "github.com/valyala/fasthttp"
)

// Globals
var (
    rdb    *redis.Client
    db     *sql.DB
    ctx_bg = context.Background()

    // LogEnvelope carries a context.Context so trace IDs propagate to goroutines
    syncChan = make(chan LogEnvelope, 1000)
    projectsLRU *lru.Cache
    // TaskChannel reserved for async work (AI content generation)
    TaskChannel = make(chan interface{}, 1024)

        // Token bucket Redis Lua (atomic) - stored also on disk as scripts/token_bucket.lua
        tokenBucketLua = `
-- Token bucket: fractional refill for smooth shaping
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_per_sec = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local tokens_needed = tonumber(ARGV[4])

local state = redis.call("HMGET", key, "tokens", "ts")
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil then tokens = capacity end
if ts == nil then ts = now end

local elapsed_ms = now - ts
local refill = (elapsed_ms / 1000.0) * refill_per_sec
tokens = math.min(capacity, tokens + refill)
local allowed = 0
if tokens >= tokens_needed then
    tokens = tokens - tokens_needed
    allowed = 1
end

redis.call("HSET", key, "tokens", tokens, "ts", now)
redis.call("EXPIRE", key, math.ceil(capacity / math.max(refill_per_sec,1))*2)
return allowed
`
        // local in-memory fallback rate limiters
        localLimiters   = make(map[string]*xrate.Limiter)
        localLimitersMu sync.Mutex
        limiterRate     = 5.0
        limiterBurst    = 50
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
    // Robust parsing of REDIS_URL; on parse failure try to sanitize then fallback to localhost
    raw := os.Getenv("REDIS_URL")
    if raw == "" {
        raw = "redis://localhost:6379/0"
    }
    // sanitize password portion if needed
    sanitized := sanitizeRedisURL(raw)
    opt, err := redis.ParseURL(sanitized)
    if err != nil {
        slog.Warn("Redis ParseURL failed after sanitize, using localhost default", "error", err, "raw", raw)
        opt = &redis.Options{Addr: "localhost:6379"}
    }
    rdb = redis.NewClient(opt)

    // Try ping with exponential backoff. If fails, set rdb=nil and start background retries.
    maxAttempts := 5
    backoff := time.Second
    var lastErr error
    for i := 0; i < maxAttempts; i++ {
        ctx, cancel := context.WithTimeout(ctx_bg, 2*time.Second)
        err := rdb.Ping(ctx).Err()
        cancel()
        if err == nil {
            // load script
            if _, lerr := rdb.ScriptLoad(ctx_bg, tokenBucketLua).Result(); lerr != nil {
                slog.Warn("Failed to load token-bucket script", "error", lerr)
            }
            slog.Info("Redis initialized", "addr", opt.Addr)
            return nil
        }
        lastErr = err
        slog.Warn("Redis ping failed, will retry", "attempt", i+1, "error", err)
        time.Sleep(backoff)
        backoff *= 2
    }

    slog.Warn("Redis unavailable after retries, starting background reconnects", "error", lastErr)
    // degrade to in-memory limiter immediately and try to reconnect in background
    rdb = nil
    go func(rawURL string) {
        attempt := 0
        b := time.Second
        for {
            attempt++
            slog.Info("attempting redis reconnect", "attempt", attempt)
            sanitized := sanitizeRedisURL(rawURL)
            opt2, perr := redis.ParseURL(sanitized)
            if perr != nil {
                slog.Warn("redis parse url failed during reconnect", "error", perr)
                time.Sleep(b)
                b = b * 2
                if b > 30*time.Second {
                    b = 30 * time.Second
                }
                continue
            }
            client := redis.NewClient(opt2)
            ctx, cancel := context.WithTimeout(ctx_bg, 3*time.Second)
            err := client.Ping(ctx).Err()
            cancel()
            if err == nil {
                // success: replace global rdb and load script
                rdb = client
                if _, lerr := rdb.ScriptLoad(ctx_bg, tokenBucketLua).Result(); lerr != nil {
                    slog.Warn("Failed to load token-bucket script after reconnect", "error", lerr)
                }
                slog.Info("Redis reconnected", "addr", opt2.Addr)
                return
            }
            slog.Warn("redis reconnect attempt failed", "error", err)
            time.Sleep(b)
            b = b * 2
            if b > 30*time.Second {
                b = 30 * time.Second
            }
        }
    }(raw)
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

    // Prefer Redis token-bucket when available
    if rdb != nil {
        res, err := rdb.Eval(ctx_bg, tokenBucketLua, []string{key}, capacity, refill, now, need).Result()
        if err == nil {
            // Redis returns integer 0/1
            switch v := res.(type) {
            case int64:
                if v == 0 {
                    ctx.SetStatusCode(fasthttp.StatusTooManyRequests)
                    ctx.SetContentType("application/json")
                    ctx.WriteString(`{"error":"rate_limit_exceeded"}`)
                    return false
                }
                return true
            case string:
                // some redis clients return string numbers
                if v == "1" {
                    return true
                }
                ctx.SetStatusCode(fasthttp.StatusTooManyRequests)
                ctx.SetContentType("application/json")
                ctx.WriteString(`{"error":"rate_limit_exceeded"}`)
                return false
            default:
                slog.Warn("Unexpected token-bucket response type", "value", res)
                // fallthrough to local limiter
            }
        } else {
            slog.Warn("Token-bucket eval failed, falling back to local limiter", "error", err)
            // fallthrough to local limiter
        }
    }

    // Local in-memory fallback limiter per IP
    limiter := getLocalLimiter(ip)
    if !limiter.Allow() {
        ctx.SetStatusCode(fasthttp.StatusTooManyRequests)
        ctx.SetContentType("application/json")
        ctx.WriteString(`{"error":"rate_limit_exceeded"}`)
        return false
    }
    return true
}

// getLocalLimiter returns or creates a per-IP in-memory rate limiter
func getLocalLimiter(ip string) *xrate.Limiter {
    localLimitersMu.Lock()
    defer localLimitersMu.Unlock()
    if l, ok := localLimiters[ip]; ok {
        return l
    }
    l := xrate.NewLimiter(xrate.Limit(limiterRate), limiterBurst)
    localLimiters[ip] = l
    return l
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

        // Use the first envelope's context to carry trace information
        spanCtx := ctx_bg
        if len(batch) > 0 && batch[0].Ctx != nil {
            spanCtx = batch[0].Ctx
        }
        spanCtx = StartSpan(spanCtx, "db.batch_insert")
        _, err := db.ExecContext(spanCtx, query, args...)
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

// setGOMAXPROCSFromCgroup adjusts GOMAXPROCS based on cgroup CPU quota if present.
func setGOMAXPROCSFromCgroup() {
    // default to NumCPU
    cpus := runtime.NumCPU()
    // Try cgroup v2 cpu.max
    if b, err := os.ReadFile("/sys/fs/cgroup/cpu.max"); err == nil {
        parts := strings.Fields(string(b))
        if len(parts) >= 1 {
            if parts[0] != "max" {
                // first value is quota (microseconds), second may be period
                quota, err1 := strconv.ParseFloat(parts[0], 64)
                period := 100000.0
                if err1 == nil {
                    if len(parts) >= 2 {
                        if p, err2 := strconv.ParseFloat(parts[1], 64); err2 == nil && p > 0 {
                            period = p
                        }
                    }
                    q := quota / period
                    if q >= 1 {
                        cpus = int(q + 0.5)
                    }
                }
            }
        }
    } else {
        // Try cgroup v1 paths
        if bq, err := os.ReadFile("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"); err == nil {
            if bp, err2 := os.ReadFile("/sys/fs/cgroup/cpu/cpu.cfs_period_us"); err2 == nil {
                if q, err3 := strconv.ParseFloat(strings.TrimSpace(string(bq)), 64); err3 == nil {
                    if p, err4 := strconv.ParseFloat(strings.TrimSpace(string(bp)), 64); err4 == nil && p > 0 {
                        if q > 0 {
                            cpus = int((q/p) + 0.5)
                        }
                    }
                }
            }
        }
    }
    if cpus < 1 {
        cpus = 1
    }
    runtime.GOMAXPROCS(cpus)
    slog.Info("GOMAXPROCS set", "procs", cpus)
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
    // Tune GOMAXPROCS according to container cgroup CPU quota (avoid excess context switches)
    setGOMAXPROCSFromCgroup()

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

    // init in-memory projects LRU cache
    if c, err := lru.New(128); err == nil {
        projectsLRU = c
    } else {
        slog.Warn("failed to init projects LRU cache", "error", err)
    }

    // Start the embedded templates HTTP server (mux) on the same addr
    port := os.Getenv("PORT")
    if port == "" {
        port = "8080"
    }
    addr := ":" + port
    // start net/http handlers (templates + API) which prefer Redis/LRU/DB
    registerHTTPHandlers(addr)

    // keep the main goroutine alive (the registered HTTP server runs in background)
    select {}
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
    // Increase pool sizes for high concurrency (TiDB-inspired tuning)
    db.SetMaxOpenConns(100)
    db.SetMaxIdleConns(50)
    db.SetConnMaxLifetime(5 * time.Minute)
    ctx, cancel := context.WithTimeout(ctx_bg, 5*time.Second)
    defer cancel()
    if err := db.PingContext(ctx); err != nil {
        return fmt.Errorf("failed to ping database: %w", err)
    }
    return nil
}

// sanitizeRedisURL attempts to URL-encode the password portion so ParseURL can handle special chars
func sanitizeRedisURL(raw string) string {
    if raw == "" {
        return raw
    }
    // quick parse: find scheme
    idx := strings.Index(raw, "://")
    if idx == -1 {
        return raw
    }
    rest := raw[idx+3:]
    at := strings.Index(rest, "@")
    if at == -1 {
        return raw
    }
    auth := rest[:at]
    host := rest[at+1:]
    if !strings.Contains(auth, ":") {
        // no password part
        return raw
    }
    parts := strings.SplitN(auth, ":", 2)
    user := parts[0]
    pass := parts[1]
    // URL-encode password
    passEnc := url.QueryEscape(pass)
    return raw[:idx+3] + user + ":" + passEnc + "@" + host
}

// Project representation for API
type Project struct {
    ID          int    `json:"id"`
    Name        string `json:"name"`
    Description string `json:"description"`
}

func handleProjects(ctx *fasthttp.RequestCtx) {
    // First try Redis cache if available
    if rdb != nil {
        if val, err := rdb.Get(ctx_bg, "projects_cache").Result(); err == nil {
            ctx.WriteString(val)
            return
        } else if err != redis.Nil {
            slog.Warn("redis get projects_cache failed", "error", err)
        }
    }

    // Then try in-memory LRU cache
    if projectsLRU != nil {
        if v, ok := projectsLRU.Get("projects"); ok {
            if s, ok := v.(string); ok {
                ctx.WriteString(s)
                return
            }
        }
    }

    // Fallback to DB
    rows, err := db.Query("SELECT id, COALESCE(name,''), COALESCE(description,'') FROM projects ORDER BY id DESC LIMIT 100")
    if err != nil {
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
    s := string(b)

    // Set caches
    if rdb != nil {
        if err := rdb.Set(ctx_bg, "projects_cache", s, 60*time.Second).Err(); err != nil {
            slog.Warn("failed to set projects_cache in redis", "error", err)
        }
    }
    if projectsLRU != nil {
        projectsLRU.Add("projects", s)
    }
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
