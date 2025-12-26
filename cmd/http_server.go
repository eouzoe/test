package main

import (
    "context"
    "embed"
    "encoding/json"
    "html/template"
    "log/slog"
    "net/http"
    "os"
    "time"
)

//go:embed templates/*.html
var templatesFS embed.FS

var tmpl *template.Template

func initTemplates() error {
    t, err := template.ParseFS(templatesFS, "templates/*.html")
    if err != nil {
        return err
    }
    tmpl = t
    return nil
}

// i18n simple map
var i18n = map[string]map[string]string{
    "zh-TW": {
        "title": "讓想法啟航",
        "description": "AI 時代，人人都能打造產品。",
        "login": "登入",
        "projects": "專案",
    },
    "en": {
        "title": "Let ideas sail",
        "description": "AI era, everyone can build products.",
        "login": "Sign in",
        "projects": "Projects",
    },
}

func localeFor(r *http.Request) string {
    q := r.URL.Query().Get("locale")
    if q != "" {
        return q
    }
    // default
    return "zh-TW"
}

func renderTemplate(w http.ResponseWriter, name string, r *http.Request, data interface{}) {
    w.Header().Set("Content-Type", "text/html; charset=utf-8")
    // Browser caching for static pages
    w.Header().Set("Cache-Control", "public, max-age=3600")
    if tmpl == nil {
        http.Error(w, "templates not initialized", http.StatusInternalServerError)
        return
    }
    // add locale data
    loc := localeFor(r)
    meta := map[string]interface{}{
        "I18n": i18n[loc],
        "Locale": loc,
    }
    // merge provided data if map
    switch d := data.(type) {
    case map[string]interface{}:
        for k, v := range meta {
            d[k] = v
        }
        tmpl.ExecuteTemplate(w, name, d)
    default:
        tmpl.ExecuteTemplate(w, name, meta)
    }
}

func registerHTTPHandlers(addr string) {
    if err := initTemplates(); err != nil {
        slog.Warn("failed to parse templates", "error", err)
    }

    mux := http.NewServeMux()

    // 1. Home
    mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
        if r.URL.Path != "/" {
            http.NotFound(w, r)
            return
        }
        renderTemplate(w, "home.html", r, nil)
    })

    // 2. Login
    mux.HandleFunc("/users/sign_in", func(w http.ResponseWriter, r *http.Request) {
        renderTemplate(w, "login.html", r, nil)
    })

    // 3. Projects page (static page that fetches via HTMX)
    mux.HandleFunc("/projects", func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Cache-Control", "public, max-age=60")
        renderTemplate(w, "projects.html", r, nil)
    })

    // API: register (JSON)
    mux.HandleFunc("/api/auth/register", func(w http.ResponseWriter, r *http.Request) {
        if r.Method != http.MethodPost {
            http.Error(w, "method", http.StatusMethodNotAllowed)
            return
        }
        var req struct{
            Username string `json:"username"`
            Password string `json:"password"`
            Email    string `json:"email"`
        }
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
            http.Error(w, "bad payload", http.StatusBadRequest)
            return
        }
        // reuse DB logic from handleRegister style but simple here
        if req.Username == "" || req.Password == "" {
            http.Error(w, "missing fields", http.StatusBadRequest)
            return
        }
        // bcrypt
        hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
        if err != nil {
            slog.Error("bcrypt failed", "error", err)
            http.Error(w, "server error", http.StatusInternalServerError)
            return
        }
        _, err = db.ExecContext(context.Background(), "INSERT INTO users (username, password_hash, email) VALUES ($1,$2,$3)", req.Username, string(hash), req.Email)
        if err != nil {
            slog.Warn("user insert failed", "error", err)
            http.Error(w, "conflict", http.StatusConflict)
            return
        }
        w.WriteHeader(http.StatusCreated)
        w.Write([]byte(`{"status":"ok"}`))
    })

    // API: projects (return JSON; prefer Redis cache)
    mux.HandleFunc("/api/projects", func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Content-Type", "application/json")
        // Try Redis
        if rdb != nil {
            if val, err := rdb.Get(context.Background(), "projects_cache").Result(); err == nil {
                w.Write([]byte(val))
                return
            }
        }
        // try LRU
        if projectsLRU != nil {
            if v, ok := projectsLRU.Get("projects"); ok {
                if s, ok := v.(string); ok {
                    w.Write([]byte(s))
                    return
                }
            }
        }
        // DB fallback
        rows, err := db.QueryContext(context.Background(), "SELECT id, COALESCE(name,''), COALESCE(description,'') FROM projects ORDER BY id DESC LIMIT 100")
        if err != nil {
            slog.Warn("projects query failed", "error", err)
            w.Write([]byte("[]"))
            return
        }
        defer rows.Close()
        var projects []Project
        for rows.Next() {
            var p Project
            if err := rows.Scan(&p.ID, &p.Name, &p.Description); err != nil {
                slog.Warn("projects scan failed", "error", err)
                continue
            }
            projects = append(projects, p)
        }
        b, _ := json.Marshal(projects)
        s := string(b)
        if rdb != nil {
            rdb.Set(context.Background(), "projects_cache", s, 60*time.Second)
        }
        if projectsLRU != nil {
            projectsLRU.Add("projects", s)
        }
        w.Write(b)
    })

    // start server
    go func(){
        slog.Info("Starting HTTP server for templates", "addr", addr)
        if err := http.ListenAndServe(addr, mux); err != nil {
            slog.Error("HTTP server failed", "error", err)
        }
    }()
}
