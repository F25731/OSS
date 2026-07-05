package main

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	ListenAddr       string
	DatabaseURL      string
	DBMaxConns       int32
	StorageDir       string
	PublicBaseURL    string
	MaxUploadBytes   int64
	AdminUser        string
	AdminPassword    string
	SessionSecret    string
	UploadKeys       map[string]string
	CORSAllowOrigins []string
}

type Server struct {
	cfg          Config
	db           *pgxpool.Pool
	cleanupRuns  atomic.Int64
	cleanupErrors atomic.Int64
}

type ImageRecord struct {
	ID           string    `json:"id"`
	PublicPath   string    `json:"publicPath"`
	URL          string    `json:"url"`
	FilePath     string    `json:"-"`
	OriginalName string    `json:"originalName"`
	SizeBytes    int64     `json:"sizeBytes"`
	MimeType     string    `json:"mimeType"`
	SHA256       string    `json:"sha256"`
	APIKeyID     string    `json:"apiKeyId,omitempty"`
	APIKeyName   string    `json:"apiKeyName"`
	CreatedAt    time.Time `json:"createdAt"`
}

type APIKey struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	KeyHash   string    `json:"-"`
	Prefix    string    `json:"prefix"`
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"createdAt"`
	LastUsedAt time.Time `json:"lastUsedAt,omitempty"`
}

type UploadLog struct {
	ID           int64     `json:"id"`
	ImageID      string    `json:"imageId"`
	APIKeyID     string    `json:"apiKeyId"`
	APIKeyName   string    `json:"apiKeyName"`
	OriginalName string    `json:"originalName"`
	SizeBytes    int64     `json:"sizeBytes"`
	MimeType      string    `json:"mimeType"`
	IP            string    `json:"ip"`
	UserAgent     string    `json:"userAgent"`
	Status        string    `json:"status"`
	Message       string    `json:"message"`
	CreatedAt     time.Time `json:"createdAt"`
}

type UploadPrincipal struct {
	ID   string
	Name string
}

type ImageFilter struct {
	Query string
	From  string
	To    string
	KeyID string
}

const adminPageSize = 20

type Pagination struct {
	Page       int
	PageSize   int
	PrevPage   int
	NextPage   int
	HasPrev    bool
	HasNext    bool
	FirstURL   string
	PrevURL    string
	NextURL    string
}

type Stats struct {
	ImageCount int64
	TotalBytes int64
}

type Settings struct {
	RetentionDays          int
	CapacityGB             int
	TrimGB                 int
	CleanupIntervalMinutes int
	CleanupBatchSize       int
	LogRetentionDays       int
	DeletedRecordRetentionDays int
}

type JSONResponse struct {
	Code int         `json:"code"`
	Data any         `json:"data"`
	Msg  string      `json:"msg"`
}

func main() {
	cfg := loadConfig()
	if err := os.MkdirAll(cfg.StorageDir, 0755); err != nil {
		log.Fatalf("create storage dir: %v", err)
	}

	ctx := context.Background()
	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("parse postgres config: %v", err)
	}
	poolCfg.MaxConns = cfg.DBMaxConns
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	srv := &Server{cfg: cfg, db: pool}
	if err := srv.migrate(ctx); err != nil {
		log.Fatalf("migrate database: %v", err)
	}

	go srv.cleanupLoop()

	log.Printf("image bed listening on %s, storage=%s", cfg.ListenAddr, cfg.StorageDir)
	httpServer := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           srv.routes(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       5 * time.Minute,
		WriteTimeout:      5 * time.Minute,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    1 << 20,
	}
	if err := httpServer.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func loadConfig() Config {
	maxMB := envInt("MAX_UPLOAD_MB", 50)
	return Config{
		ListenAddr:       env("LISTEN_ADDR", ":8080"),
		DatabaseURL:      env("DATABASE_URL", "postgres://imagebed:imagebed@localhost:5432/imagebed?sslmode=disable"),
		DBMaxConns:       int32(envInt("DB_MAX_CONNS", max(50, runtime.NumCPU()*4))),
		StorageDir:       filepath.Clean(env("STORAGE_DIR", "./data/images")),
		PublicBaseURL:    strings.TrimRight(env("PUBLIC_BASE_URL", "http://localhost:8080"), "/"),
		MaxUploadBytes:   int64(maxMB) * 1024 * 1024,
		AdminUser:        env("ADMIN_USER", "Fyanxv"),
		AdminPassword:    env("ADMIN_PASSWORD", "Fyb2530+"),
		SessionSecret:    env("SESSION_SECRET", randomHex(32)),
		UploadKeys:       parseUploadKeys(env("UPLOAD_API_KEYS", "dev-key")),
		CORSAllowOrigins: splitCSV(env("CORS_ALLOW_ORIGINS", "")),
	}
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("POST /api/upload", s.withCORS(s.handleUpload))
	mux.HandleFunc("OPTIONS /api/upload", s.withCORS(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	mux.HandleFunc("GET /api/status", s.requireAdmin(s.handleAPIStatus))
	mux.HandleFunc("GET /fyanxv/login", s.handleLoginPage)
	mux.HandleFunc("POST /fyanxv/login", s.handleLogin)
	mux.HandleFunc("POST /fyanxv/logout", s.requireAdmin(s.handleLogout))
	mux.HandleFunc("GET /fyanxv", s.requireAdmin(s.handleAdmin))
	mux.HandleFunc("GET /fyanxv/images", s.requireAdmin(s.handleImages))
	mux.HandleFunc("POST /fyanxv/images/delete", s.requireAdmin(s.handleImageDelete))
	mux.HandleFunc("GET /fyanxv/api-keys", s.requireAdmin(s.handleAPIKeys))
	mux.HandleFunc("POST /fyanxv/api-keys", s.requireAdmin(s.handleAPIKeyCreate))
	mux.HandleFunc("POST /fyanxv/api-keys/toggle", s.requireAdmin(s.handleAPIKeyToggle))
	mux.HandleFunc("POST /fyanxv/api-keys/delete", s.requireAdmin(s.handleAPIKeyDelete))
	mux.HandleFunc("GET /fyanxv/logs", s.requireAdmin(s.handleLogs))
	mux.HandleFunc("GET /fyanxv/docs", s.requireAdmin(s.handleDocs))
	mux.HandleFunc("GET /fyanxv/settings", s.requireAdmin(s.handleSettings))
	mux.HandleFunc("POST /fyanxv/settings", s.requireAdmin(s.handleSettingsUpdate))
	mux.HandleFunc("POST /fyanxv/cleanup", s.requireAdmin(s.handleCleanupNow))
	mux.Handle("/i/", http.StripPrefix("/i/", http.FileServer(http.Dir(s.cfg.StorageDir))))
	return logRequests(mux)
}

func (s *Server) migrate(ctx context.Context) error {
	stmts := []string{
		`create table if not exists images (
			id text primary key,
			public_path text not null unique,
			file_path text not null unique,
			original_name text not null,
			size_bytes bigint not null,
			mime_type text not null,
			sha256 text not null,
			api_key_id text,
			api_key_name text not null,
			created_at timestamptz not null default now(),
			deleted_at timestamptz
		)`,
		`alter table images add column if not exists api_key_id text`,
		`create index if not exists images_active_created_at_idx on images (created_at) where deleted_at is null`,
		`create index if not exists images_sha256_idx on images (sha256)`,
		`create index if not exists images_api_key_id_idx on images (api_key_id) where deleted_at is null`,
		`create table if not exists api_keys (
			id text primary key,
			name text not null,
			key_hash text not null unique,
			prefix text not null,
			enabled boolean not null default true,
			created_at timestamptz not null default now(),
			last_used_at timestamptz
		)`,
		`create index if not exists api_keys_enabled_idx on api_keys (enabled)`,
		`create table if not exists upload_logs (
			id bigserial primary key,
			image_id text,
			api_key_id text,
			api_key_name text not null default '',
			original_name text not null default '',
			size_bytes bigint not null default 0,
			mime_type text not null default '',
			ip text not null default '',
			user_agent text not null default '',
			status text not null,
			message text not null default '',
			created_at timestamptz not null default now()
		)`,
		`create index if not exists upload_logs_created_at_idx on upload_logs (created_at desc)`,
		`create index if not exists upload_logs_api_key_id_idx on upload_logs (api_key_id)`,
		`create table if not exists storage_stats (
			id int primary key check (id = 1),
			image_count bigint not null default 0,
			total_bytes bigint not null default 0,
			updated_at timestamptz not null default now()
		)`,
		`insert into storage_stats (id, image_count, total_bytes) values (1, 0, 0) on conflict (id) do nothing`,
		`create table if not exists settings (
			key text primary key,
			value text not null
		)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	defaults := map[string]string{
		"retention_days":           "7",
		"capacity_gb":              "100",
		"trim_gb":                  "30",
		"cleanup_interval_minutes": "10",
		"cleanup_batch_size":       "1000",
		"log_retention_days":       "30",
		"deleted_record_retention_days": "7",
	}
	for key, value := range defaults {
		if _, err := s.db.Exec(ctx, `insert into settings (key, value) values ($1, $2) on conflict (key) do nothing`, key, value); err != nil {
			return err
		}
	}
	if err := s.seedEnvAPIKeys(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := s.db.Ping(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, 1, nil, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, 0, map[string]string{"status": "ok"}, "ok")
}

func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.checkUploadAuth(r)
	if !ok {
		_ = s.writeUploadLog(r.Context(), UploadLog{IP: clientIP(r), UserAgent: r.UserAgent(), Status: "failed", Message: "密钥无效"})
		writeJSON(w, http.StatusUnauthorized, 1, nil, "missing or invalid upload api key")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxUploadBytes+1024*1024)
	reader, err := r.MultipartReader()
	if err != nil {
		_ = s.writeUploadLog(r.Context(), UploadLog{APIKeyID: principal.ID, APIKeyName: principal.Name, IP: clientIP(r), UserAgent: r.UserAgent(), Status: "failed", Message: "请求必须是 multipart/form-data"})
		writeJSON(w, http.StatusBadRequest, 1, nil, "expected multipart/form-data")
		return
	}

	var uploaded *ImageRecord
	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			writeJSON(w, http.StatusBadRequest, 1, nil, "failed to read multipart body")
			return
		}
		if part.FileName() == "" || (part.FormName() != "file" && part.FormName() != "image") {
			_ = part.Close()
			continue
		}
		uploaded, err = s.saveMultipartImage(r.Context(), part, principal)
		_ = part.Close()
		if err != nil {
			_ = s.writeUploadLog(r.Context(), UploadLog{APIKeyID: principal.ID, APIKeyName: principal.Name, OriginalName: part.FileName(), IP: clientIP(r), UserAgent: r.UserAgent(), Status: "failed", Message: err.Error()})
			writeJSON(w, http.StatusBadRequest, 1, nil, err.Error())
			return
		}
		break
	}
	if uploaded == nil {
		_ = s.writeUploadLog(r.Context(), UploadLog{APIKeyID: principal.ID, APIKeyName: principal.Name, IP: clientIP(r), UserAgent: r.UserAgent(), Status: "failed", Message: "缺少 file 字段"})
		writeJSON(w, http.StatusBadRequest, 1, nil, "multipart field file is required")
		return
	}
	_ = s.writeUploadLog(r.Context(), UploadLog{ImageID: uploaded.ID, APIKeyID: uploaded.APIKeyID, APIKeyName: uploaded.APIKeyName, OriginalName: uploaded.OriginalName, SizeBytes: uploaded.SizeBytes, MimeType: uploaded.MimeType, IP: clientIP(r), UserAgent: r.UserAgent(), Status: "success", Message: uploaded.URL})
	writeJSON(w, http.StatusOK, 0, uploaded, "ok")
}

func (s *Server) saveMultipartImage(ctx context.Context, part *multipart.Part, principal UploadPrincipal) (*ImageRecord, error) {
	id := randomHex(16)
	now := time.Now().UTC()
	dateDir := filepath.Join(fmt.Sprintf("%04d", now.Year()), fmt.Sprintf("%02d", int(now.Month())), fmt.Sprintf("%02d", now.Day()))
	tmpDir := filepath.Join(s.cfg.StorageDir, "_tmp")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return nil, err
	}

	tmpPath := filepath.Join(tmpDir, id+".upload")
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
	}()

	header := make([]byte, 512)
	n, readErr := io.ReadFull(part, header)
	if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
		return nil, errors.New("failed to read image")
	}
	if n == 0 {
		return nil, errors.New("empty image")
	}

	mimeType := http.DetectContentType(header[:n])
	ext, ok := imageExt(mimeType)
	if !ok {
		return nil, fmt.Errorf("unsupported image type: %s", mimeType)
	}

	hash := sha256.New()
	if _, err := tmpFile.Write(header[:n]); err != nil {
		return nil, err
	}
	if _, err := hash.Write(header[:n]); err != nil {
		return nil, err
	}
	limit := &io.LimitedReader{R: part, N: s.cfg.MaxUploadBytes - int64(n) + 1}
	copied, err := io.Copy(io.MultiWriter(tmpFile, hash), limit)
	if err != nil {
		return nil, errors.New("failed to save image")
	}
	size := int64(n) + copied
	if size > s.cfg.MaxUploadBytes || limit.N == 0 {
		return nil, fmt.Errorf("image is too large, max %s", formatBytes(s.cfg.MaxUploadBytes))
	}
	if err := tmpFile.Close(); err != nil {
		return nil, err
	}

	finalDir := filepath.Join(s.cfg.StorageDir, dateDir)
	if err := os.MkdirAll(finalDir, 0755); err != nil {
		return nil, err
	}
	fileName := id + ext
	finalPath := filepath.Join(finalDir, fileName)
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return nil, err
	}

	publicPath := "/i/" + strings.ReplaceAll(filepath.ToSlash(filepath.Join(dateDir, fileName)), "//", "/")
	record := &ImageRecord{
		ID:           id,
		PublicPath:   publicPath,
		URL:          s.cfg.PublicBaseURL + publicPath,
		FilePath:     finalPath,
		OriginalName: part.FileName(),
		SizeBytes:    size,
		MimeType:     mimeType,
		SHA256:       hex.EncodeToString(hash.Sum(nil)),
		APIKeyID:     principal.ID,
		APIKeyName:   principal.Name,
		CreatedAt:    now,
	}
	if err := s.insertImage(ctx, record); err != nil {
		_ = os.Remove(finalPath)
		return nil, err
	}
	return record, nil
}

func (s *Server) insertImage(ctx context.Context, img *ImageRecord) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, err = tx.Exec(ctx, `insert into images
		(id, public_path, file_path, original_name, size_bytes, mime_type, sha256, api_key_id, api_key_name, created_at)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		img.ID, img.PublicPath, img.FilePath, img.OriginalName, img.SizeBytes, img.MimeType, img.SHA256, img.APIKeyID, img.APIKeyName, img.CreatedAt)
	if err != nil {
		return err
	}
	if img.APIKeyID != "" {
		_, _ = tx.Exec(ctx, `update api_keys set last_used_at = now() where id = $1`, img.APIKeyID)
	}
	_, err = tx.Exec(ctx, `update storage_stats set image_count = image_count + 1, total_bytes = total_bytes + $1, updated_at = now() where id = 1`, img.SizeBytes)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Server) handleAPIStatus(w http.ResponseWriter, r *http.Request) {
	stats, err := s.readStats(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, 1, nil, err.Error())
		return
	}
	settings, err := s.readSettings(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, 1, nil, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, 0, map[string]any{
		"images":        stats.ImageCount,
		"bytes":         stats.TotalBytes,
		"humanBytes":    formatBytes(stats.TotalBytes),
		"settings":      settings,
		"cleanupRuns":   s.cleanupRuns.Load(),
		"cleanupErrors": s.cleanupErrors.Load(),
	}, "ok")
}

func (s *Server) handleLoginPage(w http.ResponseWriter, r *http.Request) {
	if s.validSession(r) {
		http.Redirect(w, r, "/fyanxv", http.StatusFound)
		return
	}
	render(w, loginTemplateV2, map[string]any{"Error": ""})
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		render(w, loginTemplateV2, map[string]any{"Error": "表单无效"})
		return
	}
	if r.FormValue("username") != s.cfg.AdminUser || r.FormValue("password") != s.cfg.AdminPassword {
		render(w, loginTemplateV2, map[string]any{"Error": "用户名或密码错误"})
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     "image_bed_session",
		Value:    s.signSession(time.Now().Add(24 * time.Hour).Unix()),
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
	http.Redirect(w, r, "/fyanxv", http.StatusFound)
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{Name: "image_bed_session", Value: "", Path: "/", MaxAge: -1})
	http.Redirect(w, r, "/fyanxv/login", http.StatusFound)
}

func (s *Server) handleAdmin(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	stats, err := s.readStats(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	recent, err := s.recentImages(ctx, 20)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	keys, err := s.listAPIKeys(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	renderAdmin(w, "overview", map[string]any{
		"Stats":         stats,
		"Recent":        recent,
		"TotalHuman":    formatBytes(stats.TotalBytes),
		"MaxUpload":     formatBytes(s.cfg.MaxUploadBytes),
		"PublicBaseURL": s.cfg.PublicBaseURL,
		"CleanupRuns":   s.cleanupRuns.Load(),
		"CleanupErrors": s.cleanupErrors.Load(),
		"APIKeyCount":   len(keys),
	})
}

func (s *Server) handleImages(w http.ResponseWriter, r *http.Request) {
	page := pageFromRequest(r)
	filter := ImageFilter{
		Query: r.URL.Query().Get("q"),
		From:  r.URL.Query().Get("from"),
		To:    r.URL.Query().Get("to"),
		KeyID: r.URL.Query().Get("key_id"),
	}
	images, err := s.searchImages(r.Context(), filter, adminPageSize+1, (page-1)*adminPageSize)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	hasNext := len(images) > adminPageSize
	if hasNext {
		images = images[:adminPageSize]
	}
	pagination := buildPagination(r, page, adminPageSize, hasNext)
	keys, err := s.listAPIKeys(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	renderAdmin(w, "images", map[string]any{"Images": images, "Filter": filter, "Keys": keys, "Pagination": pagination})
}

func (s *Server) handleImageDelete(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "表单无效", http.StatusBadRequest)
		return
	}
	id := strings.TrimSpace(r.FormValue("id"))
	if id == "" {
		http.Error(w, "缺少图片 ID", http.StatusBadRequest)
		return
	}
	if err := s.deleteImageByID(r.Context(), id); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/fyanxv/images", http.StatusFound)
}

func (s *Server) handleAPIKeys(w http.ResponseWriter, r *http.Request) {
	keys, err := s.listAPIKeys(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	renderAdmin(w, "api_keys", map[string]any{
		"Keys":       keys,
		"CreatedKey": r.URL.Query().Get("created_key"),
	})
}

func (s *Server) handleAPIKeyCreate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "表单无效", http.StatusBadRequest)
		return
	}
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		name = "未命名密钥"
	}
	raw := "ib_" + randomHex(24)
	id := "key_" + randomHex(8)
	_, err := s.db.Exec(r.Context(), `insert into api_keys (id, name, key_hash, prefix, enabled) values ($1,$2,$3,$4,true)`, id, name, hashAPIKey(raw), keyPrefix(raw))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/fyanxv/api-keys?created_key="+raw, http.StatusFound)
}

func (s *Server) handleAPIKeyToggle(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "表单无效", http.StatusBadRequest)
		return
	}
	id := strings.TrimSpace(r.FormValue("id"))
	_, err := s.db.Exec(r.Context(), `update api_keys set enabled = not enabled where id = $1`, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/fyanxv/api-keys", http.StatusFound)
}

func (s *Server) handleAPIKeyDelete(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "表单无效", http.StatusBadRequest)
		return
	}
	id := strings.TrimSpace(r.FormValue("id"))
	_, err := s.db.Exec(r.Context(), `delete from api_keys where id = $1`, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/fyanxv/api-keys", http.StatusFound)
}

func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	page := pageFromRequest(r)
	logs, err := s.listUploadLogs(r.Context(), adminPageSize+1, (page-1)*adminPageSize)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	hasNext := len(logs) > adminPageSize
	if hasNext {
		logs = logs[:adminPageSize]
	}
	pagination := buildPagination(r, page, adminPageSize, hasNext)
	renderAdmin(w, "logs", map[string]any{"Logs": logs, "Pagination": pagination})
}

func (s *Server) handleDocs(w http.ResponseWriter, r *http.Request) {
	keys, err := s.listAPIKeys(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	renderAdmin(w, "docs", map[string]any{"PublicBaseURL": s.cfg.PublicBaseURL, "Keys": keys})
}

func (s *Server) handleSettings(w http.ResponseWriter, r *http.Request) {
	settings, err := s.readSettings(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	renderAdmin(w, "settings", map[string]any{
		"Settings":      settings,
		"CapacityHuman": fmt.Sprintf("%d GB", settings.CapacityGB),
		"MaxUpload":     formatBytes(s.cfg.MaxUploadBytes),
		"PublicBaseURL": s.cfg.PublicBaseURL,
		"CleanupRuns":   s.cleanupRuns.Load(),
		"CleanupErrors": s.cleanupErrors.Load(),
	})
}

func (s *Server) handleSettingsUpdate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	settings := Settings{
		RetentionDays:          positiveFormInt(r, "retention_days", 7),
		CapacityGB:             positiveFormInt(r, "capacity_gb", 100),
		TrimGB:                 positiveFormInt(r, "trim_gb", 30),
		CleanupIntervalMinutes: positiveFormInt(r, "cleanup_interval_minutes", 10),
		CleanupBatchSize:       positiveFormInt(r, "cleanup_batch_size", 1000),
		LogRetentionDays:       positiveFormInt(r, "log_retention_days", 30),
		DeletedRecordRetentionDays: positiveFormInt(r, "deleted_record_retention_days", 7),
	}
	if err := s.saveSettings(r.Context(), settings); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/fyanxv/settings", http.StatusFound)
}

func (s *Server) handleCleanupNow(w http.ResponseWriter, r *http.Request) {
	result, err := s.runCleanup(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("manual cleanup: deleted=%d freed=%s", result.Deleted, formatBytes(result.FreedBytes))
	http.Redirect(w, r, "/fyanxv/settings", http.StatusFound)
}

func (s *Server) readStats(ctx context.Context) (Stats, error) {
	var stats Stats
	err := s.db.QueryRow(ctx, `select image_count, total_bytes from storage_stats where id = 1`).Scan(&stats.ImageCount, &stats.TotalBytes)
	return stats, err
}

func pageFromRequest(r *http.Request) int {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		return 1
	}
	return page
}

func buildPagination(r *http.Request, page int, pageSize int, hasNext bool) Pagination {
	if pageSize < 1 {
		pageSize = adminPageSize
	}
	pagination := Pagination{
		Page:       max(1, page),
		PageSize:   pageSize,
		PrevPage:   max(1, page-1),
		NextPage:   page + 1,
		HasPrev:    page > 1,
		HasNext:    hasNext,
	}
	pagination.FirstURL = pageURL(r, 1)
	if pagination.HasPrev {
		pagination.PrevURL = pageURL(r, pagination.PrevPage)
	}
	if pagination.HasNext {
		pagination.NextURL = pageURL(r, pagination.NextPage)
	}
	return pagination
}

func pageURL(r *http.Request, page int) string {
	values := r.URL.Query()
	if page <= 1 {
		values.Del("page")
	} else {
		values.Set("page", strconv.Itoa(page))
	}
	query := values.Encode()
	if query == "" {
		return r.URL.Path
	}
	return r.URL.Path + "?" + query
}

func (s *Server) readSettings(ctx context.Context) (Settings, error) {
	rows, err := s.db.Query(ctx, `select key, value from settings`)
	if err != nil {
		return Settings{}, err
	}
	defer rows.Close()
	values := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return Settings{}, err
		}
		values[k] = v
	}
	return Settings{
		RetentionDays:          positiveInt(values["retention_days"], 7),
		CapacityGB:             positiveInt(values["capacity_gb"], 100),
		TrimGB:                 positiveInt(values["trim_gb"], 30),
		CleanupIntervalMinutes: positiveInt(values["cleanup_interval_minutes"], 10),
		CleanupBatchSize:       positiveInt(values["cleanup_batch_size"], 1000),
		LogRetentionDays:       positiveInt(values["log_retention_days"], 30),
		DeletedRecordRetentionDays: positiveInt(values["deleted_record_retention_days"], 7),
	}, rows.Err()
}

func (s *Server) saveSettings(ctx context.Context, settings Settings) error {
	values := map[string]int{
		"retention_days":           settings.RetentionDays,
		"capacity_gb":              settings.CapacityGB,
		"trim_gb":                  settings.TrimGB,
		"cleanup_interval_minutes": settings.CleanupIntervalMinutes,
		"cleanup_batch_size":       settings.CleanupBatchSize,
		"log_retention_days":       settings.LogRetentionDays,
		"deleted_record_retention_days": settings.DeletedRecordRetentionDays,
	}
	for key, value := range values {
		if _, err := s.db.Exec(ctx, `insert into settings (key, value) values ($1, $2) on conflict (key) do update set value = excluded.value`, key, strconv.Itoa(value)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) recentImages(ctx context.Context, limit int) ([]ImageRecord, error) {
	rows, err := s.db.Query(ctx, `select id, public_path, file_path, original_name, size_bytes, mime_type, sha256, coalesce(api_key_id, ''), api_key_name, created_at
		from images where deleted_at is null order by created_at desc limit $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ImageRecord
	for rows.Next() {
		var item ImageRecord
		if err := rows.Scan(&item.ID, &item.PublicPath, &item.FilePath, &item.OriginalName, &item.SizeBytes, &item.MimeType, &item.SHA256, &item.APIKeyID, &item.APIKeyName, &item.CreatedAt); err != nil {
			return nil, err
		}
		item.URL = s.cfg.PublicBaseURL + item.PublicPath
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Server) imageFilterWhere(filter ImageFilter) ([]string, []any) {
	where := []string{"deleted_at is null"}
	args := []any{}
	if q := strings.TrimSpace(filter.Query); q != "" {
		args = append(args, "%"+q+"%")
		where = append(where, fmt.Sprintf("(original_name ilike $%d or id ilike $%d or sha256 ilike $%d)", len(args), len(args), len(args)))
	}
	if from := strings.TrimSpace(filter.From); from != "" {
		if t, err := time.Parse("2006-01-02", from); err == nil {
			args = append(args, t)
			where = append(where, fmt.Sprintf("created_at >= $%d", len(args)))
		}
	}
	if to := strings.TrimSpace(filter.To); to != "" {
		if t, err := time.Parse("2006-01-02", to); err == nil {
			args = append(args, t.Add(24*time.Hour))
			where = append(where, fmt.Sprintf("created_at < $%d", len(args)))
		}
	}
	if keyID := strings.TrimSpace(filter.KeyID); keyID != "" {
		args = append(args, keyID)
		where = append(where, fmt.Sprintf("api_key_id = $%d", len(args)))
	}
	return where, args
}

func (s *Server) searchImages(ctx context.Context, filter ImageFilter, limit int, offset int) ([]ImageRecord, error) {
	where, args := s.imageFilterWhere(filter)
	args = append(args, limit)
	limitArg := len(args)
	args = append(args, offset)
	query := fmt.Sprintf(`select id, public_path, file_path, original_name, size_bytes, mime_type, sha256, coalesce(api_key_id, ''), api_key_name, created_at
		from images where %s order by created_at desc limit $%d offset $%d`, strings.Join(where, " and "), limitArg, len(args))
	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ImageRecord
	for rows.Next() {
		var item ImageRecord
		if err := rows.Scan(&item.ID, &item.PublicPath, &item.FilePath, &item.OriginalName, &item.SizeBytes, &item.MimeType, &item.SHA256, &item.APIKeyID, &item.APIKeyName, &item.CreatedAt); err != nil {
			return nil, err
		}
		item.URL = s.cfg.PublicBaseURL + item.PublicPath
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Server) deleteImageByID(ctx context.Context, id string) error {
	var path string
	var size int64
	err := s.db.QueryRow(ctx, `select file_path, size_bytes from images where id = $1 and deleted_at is null`, id).Scan(&path, &size)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	_, err = s.markDeleted(ctx, id, size)
	return err
}

type CleanupResult struct {
	Deleted    int64
	FreedBytes int64
}

func (s *Server) cleanupLoop() {
	next := time.Now().Add(time.Minute)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		settings, err := s.readSettings(context.Background())
		if err != nil {
			s.cleanupErrors.Add(1)
			log.Printf("read cleanup settings: %v", err)
			continue
		}
		if time.Now().Before(next) {
			continue
		}
		result, err := s.runCleanup(context.Background())
		if err != nil {
			s.cleanupErrors.Add(1)
			log.Printf("cleanup failed: %v", err)
		} else if result.Deleted > 0 {
			log.Printf("cleanup deleted=%d freed=%s", result.Deleted, formatBytes(result.FreedBytes))
		}
		next = time.Now().Add(time.Duration(settings.CleanupIntervalMinutes) * time.Minute)
	}
}

func (s *Server) runCleanup(ctx context.Context) (CleanupResult, error) {
	settings, err := s.readSettings(ctx)
	if err != nil {
		return CleanupResult{}, err
	}
	var total CleanupResult
	if settings.RetentionDays > 0 {
		cutoff := time.Now().Add(-time.Duration(settings.RetentionDays) * 24 * time.Hour)
		result, err := s.deleteOldestWhere(ctx, `created_at < $1`, []any{cutoff}, settings.CleanupBatchSize, 0)
		if err != nil {
			return total, err
		}
		total.Deleted += result.Deleted
		total.FreedBytes += result.FreedBytes
	}

	stats, err := s.readStats(ctx)
	if err != nil {
		return total, err
	}
	capacity := int64(settings.CapacityGB) * 1024 * 1024 * 1024
	target := capacity - int64(settings.TrimGB)*1024*1024*1024
	if target < 0 {
		target = 0
	}
	if stats.TotalBytes > capacity {
		needFree := stats.TotalBytes - target
		result, err := s.deleteOldestWhere(ctx, `true`, nil, settings.CleanupBatchSize, needFree)
		if err != nil {
			return total, err
		}
		total.Deleted += result.Deleted
		total.FreedBytes += result.FreedBytes
	}
	if settings.LogRetentionDays > 0 {
		if err := s.purgeOldUploadLogs(ctx, settings); err != nil {
			return total, err
		}
	}
	if settings.DeletedRecordRetentionDays > 0 {
		if err := s.purgeDeletedImageRecords(ctx, settings); err != nil {
			return total, err
		}
	}
	s.cleanupRuns.Add(1)
	return total, nil
}

func (s *Server) purgeOldUploadLogs(ctx context.Context, settings Settings) error {
	cutoff := time.Now().Add(-time.Duration(settings.LogRetentionDays) * 24 * time.Hour)
	return s.deleteInBatches(ctx, `delete from upload_logs where id in (select id from upload_logs where created_at < $1 order by created_at asc limit $2)`, cutoff, settings.CleanupBatchSize)
}

func (s *Server) purgeDeletedImageRecords(ctx context.Context, settings Settings) error {
	cutoff := time.Now().Add(-time.Duration(settings.DeletedRecordRetentionDays) * 24 * time.Hour)
	return s.deleteInBatches(ctx, `delete from images where id in (select id from images where deleted_at is not null and deleted_at < $1 order by deleted_at asc limit $2)`, cutoff, settings.CleanupBatchSize)
}

func (s *Server) deleteInBatches(ctx context.Context, query string, cutoff time.Time, batchSize int) error {
	if batchSize <= 0 {
		batchSize = 1000
	}
	for {
		tag, err := s.db.Exec(ctx, query, cutoff, batchSize)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil
		}
	}
}

func (s *Server) deleteOldestWhere(ctx context.Context, where string, args []any, batchSize int, stopAfterBytes int64) (CleanupResult, error) {
	if batchSize <= 0 {
		batchSize = 1000
	}
	var total CleanupResult
	for {
		query := fmt.Sprintf(`select id, file_path, size_bytes from images where deleted_at is null and %s order by created_at asc limit $%d`, where, len(args)+1)
		rows, err := s.db.Query(ctx, query, append(args, batchSize)...)
		if err != nil {
			return total, err
		}
		type candidate struct {
			id   string
			path string
			size int64
		}
		var items []candidate
		for rows.Next() {
			var item candidate
			if err := rows.Scan(&item.id, &item.path, &item.size); err != nil {
				rows.Close()
				return total, err
			}
			items = append(items, item)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return total, err
		}
		rows.Close()
		if len(items) == 0 {
			return total, nil
		}
		for _, item := range items {
			if err := os.Remove(item.path); err != nil && !errors.Is(err, os.ErrNotExist) {
				log.Printf("remove image file failed path=%s err=%v", item.path, err)
				continue
			}
			deleted, err := s.markDeleted(ctx, item.id, item.size)
			if err != nil {
				return total, err
			}
			if deleted {
				total.Deleted++
				total.FreedBytes += item.size
				if stopAfterBytes > 0 && total.FreedBytes >= stopAfterBytes {
					return total, nil
				}
			}
		}
	}
}

func (s *Server) markDeleted(ctx context.Context, id string, size int64) (bool, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `update images set deleted_at = now() where id = $1 and deleted_at is null`, id)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, tx.Commit(ctx)
	}
	_, err = tx.Exec(ctx, `update storage_stats set image_count = greatest(image_count - 1, 0::bigint), total_bytes = greatest(total_bytes - $1, 0::bigint), updated_at = now() where id = 1`, size)
	if err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

func (s *Server) checkUploadAuth(r *http.Request) (UploadPrincipal, bool) {
	key := strings.TrimSpace(r.Header.Get("X-API-Key"))
	if key == "" {
		auth := r.Header.Get("Authorization")
		if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
			key = strings.TrimSpace(auth[7:])
		}
	}
	if key == "" {
		return UploadPrincipal{}, false
	}
	hash := hashAPIKey(key)
	var principal UploadPrincipal
	var enabled bool
	err := s.db.QueryRow(r.Context(), `select id, name, enabled from api_keys where key_hash = $1`, hash).Scan(&principal.ID, &principal.Name, &enabled)
	if err == nil {
		if enabled {
			return principal, true
		}
		return UploadPrincipal{}, false
	}
	return UploadPrincipal{}, false
}

func (s *Server) seedEnvAPIKeys(ctx context.Context) error {
	for key, name := range s.cfg.UploadKeys {
		if key == "" {
			continue
		}
		id := "key_" + randomHex(8)
		_, err := s.db.Exec(ctx, `insert into api_keys (id, name, key_hash, prefix, enabled)
			values ($1, $2, $3, $4, true) on conflict (key_hash) do nothing`, id, name, hashAPIKey(key), keyPrefix(key))
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) writeUploadLog(ctx context.Context, item UploadLog) error {
	_, err := s.db.Exec(ctx, `insert into upload_logs
		(image_id, api_key_id, api_key_name, original_name, size_bytes, mime_type, ip, user_agent, status, message)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		emptyToNil(item.ImageID), emptyToNil(item.APIKeyID), item.APIKeyName, item.OriginalName, item.SizeBytes, item.MimeType, item.IP, item.UserAgent, item.Status, item.Message)
	return err
}

func (s *Server) listAPIKeys(ctx context.Context) ([]APIKey, error) {
	rows, err := s.db.Query(ctx, `select id, name, key_hash, prefix, enabled, created_at, coalesce(last_used_at, '0001-01-01 00:00:00+00'::timestamptz) from api_keys order by created_at desc`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []APIKey
	for rows.Next() {
		var item APIKey
		if err := rows.Scan(&item.ID, &item.Name, &item.KeyHash, &item.Prefix, &item.Enabled, &item.CreatedAt, &item.LastUsedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Server) listUploadLogs(ctx context.Context, limit int, offset int) ([]UploadLog, error) {
	rows, err := s.db.Query(ctx, `select id, coalesce(image_id, ''), coalesce(api_key_id, ''), api_key_name, original_name, size_bytes, mime_type, ip, user_agent, status, message, created_at
		from upload_logs order by created_at desc limit $1 offset $2`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []UploadLog
	for rows.Next() {
		var item UploadLog
		if err := rows.Scan(&item.ID, &item.ImageID, &item.APIKeyID, &item.APIKeyName, &item.OriginalName, &item.SizeBytes, &item.MimeType, &item.IP, &item.UserAgent, &item.Status, &item.Message, &item.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func hashAPIKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func keyPrefix(key string) string {
	if len(key) <= 10 {
		return key
	}
	return key[:6] + "..." + key[len(key)-4:]
}

func emptyToNil(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func clientIP(r *http.Request) string {
	for _, header := range []string{"X-Forwarded-For", "X-Real-IP"} {
		value := strings.TrimSpace(r.Header.Get(header))
		if value == "" {
			continue
		}
		return strings.TrimSpace(strings.Split(value, ",")[0])
	}
	host := r.RemoteAddr
	if index := strings.LastIndex(host, ":"); index > -1 {
		return host[:index]
	}
	return host
}

func (s *Server) withCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if s.originAllowed(origin) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, X-API-Key, Content-Type")
			w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		}
		next(w, r)
	}
}

func (s *Server) originAllowed(origin string) bool {
	if origin == "" || len(s.cfg.CORSAllowOrigins) == 0 {
		return false
	}
	for _, item := range s.cfg.CORSAllowOrigins {
		if item == "*" || item == origin {
			return true
		}
	}
	return false
}

func (s *Server) requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.validSession(r) {
			http.Redirect(w, r, "/fyanxv/login", http.StatusFound)
			return
		}
		next(w, r)
	}
}

func (s *Server) signSession(exp int64) string {
	data := fmt.Sprintf("%s:%d", s.cfg.AdminUser, exp)
	mac := hmac.New(sha256.New, []byte(s.cfg.SessionSecret))
	_, _ = mac.Write([]byte(data))
	return data + ":" + hex.EncodeToString(mac.Sum(nil))
}

func (s *Server) validSession(r *http.Request) bool {
	cookie, err := r.Cookie("image_bed_session")
	if err != nil {
		return false
	}
	parts := strings.Split(cookie.Value, ":")
	if len(parts) != 3 || parts[0] != s.cfg.AdminUser {
		return false
	}
	exp, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || time.Now().Unix() > exp {
		return false
	}
	expected := s.signSession(exp)
	return hmac.Equal([]byte(expected), []byte(cookie.Value))
}

func imageExt(mimeType string) (string, bool) {
	switch mimeType {
	case "image/jpeg":
		return ".jpg", true
	case "image/png":
		return ".png", true
	case "image/webp":
		return ".webp", true
	case "image/gif":
		return ".gif", true
	default:
		return "", false
	}
}

func writeJSON(w http.ResponseWriter, status int, code int, data any, msg string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(JSONResponse{Code: code, Data: data, Msg: msg})
}

func render(w http.ResponseWriter, raw string, data any) {
	tpl := template.Must(template.New("page").Funcs(template.FuncMap{
		"bytes": formatBytes,
	}).Parse(raw))
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tpl.Execute(w, data); err != nil {
		log.Printf("render page: %v", err)
	}
}

func renderAdmin(w http.ResponseWriter, page string, data map[string]any) {
	if data == nil {
		data = map[string]any{}
	}
	data["Page"] = page
	tpl := template.Must(template.New("admin").Funcs(template.FuncMap{
		"bytes": formatBytes,
		"date":  formatTime,
		"short": shortText,
	}).Parse(adminTemplateV2))
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tpl.Execute(w, data); err != nil {
		log.Printf("render admin page: %v", err)
	}
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start).Round(time.Millisecond))
	})
}

func env(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func envInt(key string, fallback int) int {
	return positiveInt(os.Getenv(key), fallback)
}

func positiveInt(value string, fallback int) int {
	n, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func positiveFormInt(r *http.Request, key string, fallback int) int {
	return positiveInt(r.FormValue(key), fallback)
}

func parseUploadKeys(value string) map[string]string {
	result := map[string]string{}
	for index, item := range splitCSV(value) {
		parts := strings.SplitN(item, ":", 2)
		key := strings.TrimSpace(parts[0])
		if key == "" {
			continue
		}
		name := fmt.Sprintf("key-%d", index+1)
		if len(parts) == 2 && strings.TrimSpace(parts[1]) != "" {
			name = strings.TrimSpace(parts[1])
		}
		result[key] = name
	}
	return result
}

func splitCSV(value string) []string {
	var result []string
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			result = append(result, item)
		}
	}
	return result
}

func randomHex(bytes int) string {
	buf := make([]byte, bytes)
	if _, err := rand.Read(buf); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(buf)
}

func formatBytes(bytes int64) string {
	if bytes < 0 {
		bytes = 0
	}
	units := []string{"B", "KB", "MB", "GB", "TB"}
	value := float64(bytes)
	unit := 0
	for value >= 1024 && unit < len(units)-1 {
		value /= 1024
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%d %s", bytes, units[unit])
	}
	return fmt.Sprintf("%.2f %s", value, units[unit])
}

func formatTime(value any) string {
	switch typed := value.(type) {
	case time.Time:
		if typed.IsZero() {
			return "-"
		}
		return typed.Local().Format("2006-01-02 15:04:05")
	case *time.Time:
		if typed == nil || typed.IsZero() {
			return "-"
		}
		return typed.Local().Format("2006-01-02 15:04:05")
	default:
		return "-"
	}
}

func shortText(value string, max int) string {
	value = strings.TrimSpace(value)
	if max <= 0 || len([]rune(value)) <= max {
		return value
	}
	runes := []rune(value)
	return string(runes[:max]) + "..."
}

const loginTemplate = `<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Fyanxv 登录</title>
<style>
body{margin:0;font-family:Inter,Arial,sans-serif;background:#f5f7fb;color:#172033;display:grid;place-items:center;min-height:100vh}
.box{width:min(360px,calc(100vw - 32px));background:#fff;border:1px solid #dce3ee;border-radius:8px;padding:24px;box-shadow:0 18px 50px #20305018}
h1{font-size:22px;margin:0 0 18px}
label{display:block;font-size:13px;color:#536175;margin:14px 0 6px}
input{width:100%;box-sizing:border-box;border:1px solid #cfd8e5;border-radius:6px;padding:10px 12px;font-size:14px}
button{width:100%;border:0;border-radius:6px;background:#1769e0;color:#fff;padding:11px 12px;margin-top:18px;font-weight:700;cursor:pointer}
.err{background:#fff1f0;border:1px solid #ffccc7;color:#a8071a;border-radius:6px;padding:8px 10px;font-size:13px;margin-bottom:12px}
</style>
</head>
<body>
<form class="box" method="post" action="/fyanxv/login">
<h1>Fyanxv</h1>
{{if .Error}}<div class="err">{{.Error}}</div>{{end}}
<label>用户名</label><input name="username" autocomplete="username" required>
<label>密码</label><input type="password" name="password" autocomplete="current-password" required>
<button type="submit">登录</button>
</form>
</body>
</html>`

const loginTemplateV2 = `<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Fyanxv 登录</title>
<style>
body{margin:0;font-family:Inter,Arial,"Microsoft YaHei",sans-serif;background:#f5f7fb;color:#172033;display:grid;place-items:center;min-height:100vh}
.box{width:min(360px,calc(100vw - 32px));background:#fff;border:1px solid #dce3ee;border-radius:8px;padding:24px;box-shadow:0 18px 50px #20305018}
h1{font-size:22px;margin:0 0 18px}
label{display:block;font-size:13px;color:#536175;margin:14px 0 6px}
input{width:100%;box-sizing:border-box;border:1px solid #cfd8e5;border-radius:6px;padding:10px 12px;font-size:14px}
button{width:100%;border:0;border-radius:6px;background:#1769e0;color:#fff;padding:11px 12px;margin-top:18px;font-weight:700;cursor:pointer}
.err{background:#fff1f0;border:1px solid #ffccc7;color:#a8071a;border-radius:6px;padding:8px 10px;font-size:13px;margin-bottom:12px}
</style>
</head>
<body>
<form class="box" method="post" action="/fyanxv/login">
<h1>Fyanxv</h1>
{{if .Error}}<div class="err">{{.Error}}</div>{{end}}
<label>用户名</label><input name="username" autocomplete="username" required>
<label>密码</label><input type="password" name="password" autocomplete="current-password" required>
<button type="submit">登录</button>
</form>
</body>
</html>`

const adminTemplateV2 = `<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Fyanxv 后台</title>
<style>
*{box-sizing:border-box}
body{margin:0;font-family:Inter,Arial,"Microsoft YaHei",sans-serif;background:#f4f7fb;color:#172033}
.layout{display:flex;min-height:100vh}
.side{width:228px;background:#111827;color:#d9e2f2;padding:18px 14px;position:sticky;top:0;height:100vh}
.brand{display:flex;align-items:center;gap:10px;font-weight:800;font-size:18px;color:#fff;margin:4px 8px 20px}
.brand img{width:28px;height:28px;display:block}
.nav a{display:block;color:#d9e2f2;text-decoration:none;padding:10px 12px;border-radius:6px;margin:4px 0;font-size:14px}
.nav a.active,.nav a:hover{background:#1f6feb;color:#fff}
.main{flex:1;min-width:0}
.top{height:56px;background:#fff;border-bottom:1px solid #dce3ee;display:flex;align-items:center;justify-content:space-between;padding:0 24px}
.top h1{font-size:18px;margin:0}
.content{padding:22px;max-width:1280px}
.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px}
.card{background:#fff;border:1px solid #dce3ee;border-radius:8px;padding:16px;margin-bottom:16px}
.k{font-size:12px;color:#65758b}.v{font-size:24px;font-weight:800;margin-top:8px}
h2{font-size:16px;margin:0 0 14px}
form.inline{display:inline}
button,.btn{border:0;border-radius:6px;background:#1769e0;color:#fff;padding:8px 12px;font-weight:700;cursor:pointer;text-decoration:none;display:inline-block}
button.secondary,.btn.secondary{background:#eef3fb;color:#1f2a44}
button.danger{background:#d93025}
button[disabled]{opacity:.65;cursor:not-allowed}
input,select{border:1px solid #cfd8e5;border-radius:6px;padding:8px 10px;font-size:14px;width:100%}
label{display:block;font-size:12px;color:#536175;margin:8px 0 5px}
.row{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:12px;align-items:end}
.section-head{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:14px}
.section-head h2{margin:0}
.pager{display:flex;align-items:center;justify-content:flex-end;gap:10px;margin-top:14px;color:#65758b;font-size:13px}
.pager .disabled{opacity:.45;pointer-events:none}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{padding:10px;border-bottom:1px solid #e7edf5;text-align:left;vertical-align:middle}
th{color:#536175;font-weight:700;background:#fafcff}
a{color:#1769e0;text-decoration:none}
.thumb{width:52px;height:52px;object-fit:cover;border-radius:6px;background:#eef3fb}
code,pre{background:#eef3fb;border-radius:6px}
code{padding:2px 5px}
pre{padding:14px;overflow:auto;line-height:1.6}
.alert{background:#e8f7ee;border:1px solid #b7ebc6;color:#135c2c;padding:12px;border-radius:8px;margin-bottom:16px}
.muted{color:#65758b}
.tag{display:inline-block;padding:2px 7px;border-radius:999px;background:#eef3fb;color:#1f2a44}
.tag.ok{background:#e8f7ee;color:#137333}.tag.bad{background:#fff1f0;color:#a8071a}
@media(max-width:900px){.layout{display:block}.side{position:relative;width:auto;height:auto}.grid{grid-template-columns:repeat(2,minmax(0,1fr))}.row{grid-template-columns:1fr 1fr}.content{padding:14px}}
</style>
</head>
<body>
<div class="layout">
<aside class="side">
<div class="brand"><img src="https://www.tfbkw.com/wp-content/themes/ZibTF/img/zm.svg" alt="Fyanxv"><span>Fyanxv</span></div>
<nav class="nav">
<a class="{{if eq .Page "overview"}}active{{end}}" href="/fyanxv">概览</a>
<a class="{{if eq .Page "images"}}active{{end}}" href="/fyanxv/images">图片管理</a>
<a class="{{if eq .Page "api_keys"}}active{{end}}" href="/fyanxv/api-keys">API 密钥</a>
<a class="{{if eq .Page "logs"}}active{{end}}" href="/fyanxv/logs">上传日志</a>
<a class="{{if eq .Page "docs"}}active{{end}}" href="/fyanxv/docs">接入文档</a>
<a class="{{if eq .Page "settings"}}active{{end}}" href="/fyanxv/settings">系统设置</a>
</nav>
</aside>
<main class="main">
<header class="top">
<h1>{{if eq .Page "overview"}}概览{{else if eq .Page "images"}}图片管理{{else if eq .Page "api_keys"}}API 密钥{{else if eq .Page "logs"}}上传日志{{else if eq .Page "docs"}}接入文档{{else}}系统设置{{end}}</h1>
<form class="inline" method="post" action="/fyanxv/logout"><button class="secondary">退出登录</button></form>
</header>
<section class="content">

{{if eq .Page "overview"}}
<div class="section-head"><h2>实时状态</h2><button type="button" class="secondary" data-refresh>刷新</button></div>
<section class="grid">
<div class="card"><div class="k">当前图片数</div><div class="v">{{.Stats.ImageCount}}</div></div>
<div class="card"><div class="k">已用容量</div><div class="v">{{.TotalHuman}}</div></div>
<div class="card"><div class="k">API 密钥数</div><div class="v">{{.APIKeyCount}}</div></div>
<div class="card"><div class="k">单图上传上限</div><div class="v">{{.MaxUpload}}</div></div>
</section>
<section class="card">
<h2>最近上传图片</h2>
<table><thead><tr><th>预览</th><th>地址</th><th>大小</th><th>密钥</th><th>上传时间</th></tr></thead><tbody>
{{range .Recent}}<tr><td><a href="{{.URL}}" target="_blank"><img class="thumb" src="{{.PublicPath}}" alt=""></a></td><td><a href="{{.URL}}" target="_blank">{{.PublicPath}}</a></td><td>{{bytes .SizeBytes}}</td><td>{{.APIKeyName}}</td><td>{{date .CreatedAt}}</td></tr>{{else}}<tr><td colspan="5">还没有上传图片。</td></tr>{{end}}
</tbody></table>
</section>
{{end}}

{{if eq .Page "images"}}
<div class="section-head"><h2>图片管理</h2><button type="button" class="secondary" data-refresh>刷新</button></div>
<section class="card">
<h2>搜索筛选</h2>
<form method="get" action="/fyanxv/images" class="row">
<div><label>关键词</label><input name="q" value="{{.Filter.Query}}" placeholder="文件名、ID、SHA256"></div>
<div><label>开始日期</label><input type="date" name="from" value="{{.Filter.From}}"></div>
<div><label>结束日期</label><input type="date" name="to" value="{{.Filter.To}}"></div>
<div><label>API 密钥</label><select name="key_id"><option value="">全部</option>{{range .Keys}}<option value="{{.ID}}" {{if eq $.Filter.KeyID .ID}}selected{{end}}>{{.Name}}（{{.Prefix}}）</option>{{end}}</select></div>
<div><button type="submit">查询</button></div>
</form>
</section>
<section class="card">
<h2>图片列表</h2>
<table><thead><tr><th>预览</th><th>文件</th><th>大小</th><th>类型</th><th>密钥</th><th>上传时间</th><th>操作</th></tr></thead><tbody>
{{range .Images}}<tr>
<td><a href="{{.URL}}" target="_blank"><img class="thumb" src="{{.PublicPath}}" alt=""></a></td>
<td><a href="{{.URL}}" target="_blank">{{.OriginalName}}</a><br><span class="muted">{{.PublicPath}}</span></td>
<td>{{bytes .SizeBytes}}</td><td>{{.MimeType}}</td><td>{{.APIKeyName}}</td><td>{{date .CreatedAt}}</td>
<td><form method="post" action="/fyanxv/images/delete" onsubmit="return confirm('确定删除这张图片吗？')"><input type="hidden" name="id" value="{{.ID}}"><button class="danger" type="submit">删除</button></form></td>
</tr>{{else}}<tr><td colspan="7">没有找到图片。</td></tr>{{end}}
</tbody></table>
</section>
{{end}}

{{if and (eq .Page "images") .Pagination}}<div class="pager"><span>第 {{.Pagination.Page}} 页，每页 {{.Pagination.PageSize}} 条</span><a class="btn secondary {{if not .Pagination.HasPrev}}disabled{{end}}" href="{{.Pagination.FirstURL}}">第一页</a><a class="btn secondary {{if not .Pagination.HasPrev}}disabled{{end}}" href="{{.Pagination.PrevURL}}">上一页</a><a class="btn secondary {{if not .Pagination.HasNext}}disabled{{end}}" href="{{.Pagination.NextURL}}">下一页</a></div>{{end}}
{{if eq .Page "api_keys"}}
{{if .CreatedKey}}<div class="alert"><strong>新密钥已生成，只显示这一次：</strong><br><code>{{.CreatedKey}}</code></div>{{end}}
<section class="card">
<h2>生成 API 密钥</h2>
<form method="post" action="/fyanxv/api-keys" class="row">
<div style="grid-column:span 4"><label>密钥名称</label><input name="name" placeholder="例如：画布上传、测试环境"></div>
<div><button type="submit">生成密钥</button></div>
</form>
</section>
<section class="card">
<h2>密钥列表</h2>
<table><thead><tr><th>名称</th><th>前缀</th><th>状态</th><th>创建时间</th><th>最后使用</th><th>操作</th></tr></thead><tbody>
{{range .Keys}}<tr>
<td>{{.Name}}</td><td><code>{{.Prefix}}</code></td><td>{{if .Enabled}}<span class="tag ok">启用</span>{{else}}<span class="tag bad">停用</span>{{end}}</td><td>{{date .CreatedAt}}</td><td>{{date .LastUsedAt}}</td>
<td><form class="inline" method="post" action="/fyanxv/api-keys/toggle"><input type="hidden" name="id" value="{{.ID}}"><button class="secondary" type="submit">{{if .Enabled}}停用{{else}}启用{{end}}</button></form> <form class="inline" method="post" action="/fyanxv/api-keys/delete" onsubmit="return confirm('确定删除这个密钥吗？')"><input type="hidden" name="id" value="{{.ID}}"><button class="danger" type="submit">删除</button></form></td>
</tr>{{else}}<tr><td colspan="6">还没有密钥。</td></tr>{{end}}
</tbody></table>
</section>
{{end}}

{{if eq .Page "logs"}}
<div class="section-head"><h2>上传日志</h2><button type="button" class="secondary" data-refresh>刷新</button></div>
<section class="card">
<h2>上传日志</h2>
<table><thead><tr><th>状态</th><th>文件</th><th>大小</th><th>类型</th><th>密钥</th><th>IP</th><th>信息</th><th>时间</th></tr></thead><tbody>
{{range .Logs}}<tr><td>{{if eq .Status "success"}}<span class="tag ok">成功</span>{{else}}<span class="tag bad">失败</span>{{end}}</td><td>{{.OriginalName}}</td><td>{{bytes .SizeBytes}}</td><td>{{.MimeType}}</td><td>{{.APIKeyName}}</td><td>{{.IP}}</td><td title="{{.Message}}">{{short .Message 42}}</td><td>{{date .CreatedAt}}</td></tr>{{else}}<tr><td colspan="8">暂无日志。</td></tr>{{end}}
</tbody></table>
</section>
{{end}}

{{if and (eq .Page "logs") .Pagination}}<div class="pager"><span>第 {{.Pagination.Page}} 页，每页 {{.Pagination.PageSize}} 条</span><a class="btn secondary {{if not .Pagination.HasPrev}}disabled{{end}}" href="{{.Pagination.FirstURL}}">第一页</a><a class="btn secondary {{if not .Pagination.HasPrev}}disabled{{end}}" href="{{.Pagination.PrevURL}}">上一页</a><a class="btn secondary {{if not .Pagination.HasNext}}disabled{{end}}" href="{{.Pagination.NextURL}}">下一页</a></div>{{end}}
{{if eq .Page "docs"}}
<section class="card">
<h2>上传接口</h2>
<p>接口地址：<code>{{.PublicBaseURL}}/api/upload</code></p>
<p>请求方式：<code>POST multipart/form-data</code>，文件字段名使用 <code>file</code> 或 <code>image</code>。</p>
<p>鉴权方式：请求头 <code>Authorization: Bearer 你的_API_KEY</code>，也支持 <code>X-API-Key: 你的_API_KEY</code>。</p>
<pre>curl -X POST {{.PublicBaseURL}}/api/upload \
  -H "Authorization: Bearer 你的_API_KEY" \
  -F "file=@./test.png"</pre>
<p>成功返回：</p>
<pre>{
  "code": 0,
  "data": {
    "url": "{{.PublicBaseURL}}/i/2026/07/05/example.png",
    "publicPath": "/i/2026/07/05/example.png",
    "sizeBytes": 12345,
    "mimeType": "image/png"
  },
  "msg": "ok"
}</pre>
<p class="muted">给 AI 使用时，直接取返回里的 <code>data.url</code> 作为参考图地址。</p>
</section>
{{end}}

{{if eq .Page "settings"}}
<section class="card">
<h2>清理设置</h2>
<form method="post" action="/fyanxv/settings" class="row">
<div><label>保留天数</label><input type="number" min="1" name="retention_days" value="{{.Settings.RetentionDays}}"></div>
<div><label>容量上限 GB</label><input type="number" min="1" name="capacity_gb" value="{{.Settings.CapacityGB}}"></div>
<div><label>超限后清理 GB</label><input type="number" min="1" name="trim_gb" value="{{.Settings.TrimGB}}"></div>
<div><label>清理间隔分钟</label><input type="number" min="1" name="cleanup_interval_minutes" value="{{.Settings.CleanupIntervalMinutes}}"></div>
<div><label>每批清理数量</label><input type="number" min="100" name="cleanup_batch_size" value="{{.Settings.CleanupBatchSize}}"></div>
<div><label>日志保留天数</label><input type="number" min="1" name="log_retention_days" value="{{.Settings.LogRetentionDays}}"></div>
<div><label>删除记录保留天数</label><input type="number" min="1" name="deleted_record_retention_days" value="{{.Settings.DeletedRecordRetentionDays}}"></div>
<div><button type="submit">保存设置</button></div>
</form>
<form method="post" action="/fyanxv/cleanup" style="margin-top:12px"><button class="secondary" type="submit">立即执行清理</button></form>
<p class="muted">当前公网地址前缀：<code>{{.PublicBaseURL}}</code>。清理次数：{{.CleanupRuns}}，清理错误：{{.CleanupErrors}}。</p>
</section>
{{end}}

</section>
</main>
</div>
<script>
document.addEventListener("click", async function(event) {
  const button = event.target.closest("[data-refresh]");
  if (!button) return;
  event.preventDefault();
  const content = document.querySelector(".content");
  if (!content) return;
  const oldText = button.textContent;
  button.disabled = true;
  button.textContent = "刷新中";
  try {
    const response = await fetch(window.location.href, {headers: {"X-Requested-With": "fetch"}});
    const html = await response.text();
    const doc = new DOMParser().parseFromString(html, "text/html");
    const nextContent = doc.querySelector(".content");
    if (nextContent) content.innerHTML = nextContent.innerHTML;
  } finally {
    button.disabled = false;
    button.textContent = oldText || "刷新";
  }
});
</script>
</body>
</html>`

const adminTemplate = `<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Fyanxv 后台</title>
<style>
body{margin:0;font-family:Inter,Arial,sans-serif;background:#f5f7fb;color:#172033}
header{height:56px;background:#111827;color:#fff;display:flex;align-items:center;justify-content:space-between;padding:0 24px}
main{max-width:1180px;margin:24px auto;padding:0 18px}
.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px}
.card{background:#fff;border:1px solid #dce3ee;border-radius:8px;padding:16px}
.k{font-size:12px;color:#65758b}.v{font-size:24px;font-weight:800;margin-top:8px}
h2{font-size:16px;margin:0 0 14px}
form.inline{display:inline}
button{border:0;border-radius:6px;background:#1769e0;color:#fff;padding:9px 12px;font-weight:700;cursor:pointer}
button.secondary{background:#eef3fb;color:#1f2a44}
input{border:1px solid #cfd8e5;border-radius:6px;padding:8px 10px;font-size:14px;width:100%;box-sizing:border-box}
label{display:block;font-size:12px;color:#536175;margin:10px 0 5px}
.settings{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:12px;align-items:end}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{padding:10px;border-bottom:1px solid #e7edf5;text-align:left;vertical-align:middle}
th{color:#536175;font-weight:700}
a{color:#1769e0;text-decoration:none}
.thumb{width:48px;height:48px;object-fit:cover;border-radius:6px;background:#eef3fb}
code{background:#eef3fb;padding:2px 5px;border-radius:4px}
@media(max-width:900px){.grid{grid-template-columns:repeat(2,minmax(0,1fr))}.settings{grid-template-columns:1fr 1fr}}
</style>
</head>
<body>
<header>
<strong>Fyanxv</strong>
<form class="inline" method="post" action="/fyanxv/logout"><button class="secondary">退出登录</button></form>
</header>
<main>
<section class="grid">
<div class="card"><div class="k">当前图片数</div><div class="v">{{.Stats.ImageCount}}</div></div>
<div class="card"><div class="k">已用容量</div><div class="v">{{.TotalHuman}}</div></div>
<div class="card"><div class="k">容量上限</div><div class="v">{{.CapacityHuman}}</div></div>
<div class="card"><div class="k">单图上传上限</div><div class="v">{{.MaxUpload}}</div></div>
</section>

<section class="card" style="margin-top:16px">
<h2>清理设置</h2>
<form method="post" action="/fyanxv/settings" class="settings">
<div><label>保留天数</label><input type="number" min="1" name="retention_days" value="{{.Settings.RetentionDays}}"></div>
<div><label>容量上限 GB</label><input type="number" min="1" name="capacity_gb" value="{{.Settings.CapacityGB}}"></div>
<div><label>超限后清理 GB</label><input type="number" min="1" name="trim_gb" value="{{.Settings.TrimGB}}"></div>
<div><label>清理间隔分钟</label><input type="number" min="1" name="cleanup_interval_minutes" value="{{.Settings.CleanupIntervalMinutes}}"></div>
<div><label>每批清理数量</label><input type="number" min="100" name="cleanup_batch_size" value="{{.Settings.CleanupBatchSize}}"></div>
<div><button type="submit">保存设置</button></div>
</form>
<form method="post" action="/fyanxv/cleanup" style="margin-top:12px"><button class="secondary" type="submit">立即执行清理</button></form>
<p class="k">清理次数：{{.CleanupRuns}}，清理错误：{{.CleanupErrors}}。公网地址前缀：<code>{{.PublicBaseURL}}</code></p>
</section>

<section class="card" style="margin-top:16px">
<h2>最近上传</h2>
<table>
<thead><tr><th>预览</th><th>地址</th><th>大小</th><th>类型</th><th>接口密钥</th><th>上传时间</th></tr></thead>
<tbody>
{{range .Recent}}
<tr>
<td><a href="{{.URL}}" target="_blank"><img class="thumb" src="{{.PublicPath}}" alt=""></a></td>
<td><a href="{{.URL}}" target="_blank">{{.PublicPath}}</a></td>
<td>{{bytes .SizeBytes}}</td>
<td>{{.MimeType}}</td>
<td>{{.APIKeyName}}</td>
<td>{{.CreatedAt.Format "2006-01-02 15:04:05"}}</td>
</tr>
{{else}}<tr><td colspan="6">还没有上传图片。</td></tr>{{end}}
</tbody>
</table>
</section>
</main>
</body>
</html>`
