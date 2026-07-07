package main

import (
	"context"
	"crypto/hmac"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"log"
	"math/rand"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	imageassets "ai-image-bed/internal/assets"
	imagemigrations "ai-image-bed/internal/migrations"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	ListenAddr                     string
	DatabaseURL                    string
	DBMaxConns                     int32
	StorageDir                     string
	PublicBaseURL                  string
	MaxUploadBytes                 int64
	MaxConcurrentUploads           int
	MaxQueuedUploads               int
	UploadQueueTimeout            time.Duration
	UploadRateLimitPerKeyPerMinute int
	UploadRateLimitPerIPPerMinute  int
	UploadLogQueueSize             int
	SuccessUploadLogSamplePercent  int
	AdminUser                      string
	AdminPassword                  string
	SessionSecret                  string
	UploadKeys                     map[string]string
	CORSAllowOrigins               []string
}

type Server struct {
	cfg              Config
	db               *pgxpool.Pool
	cleanupRuns      atomic.Int64
	cleanupErrors    atomic.Int64
	cleanupActive    atomic.Bool
	statsFlushActive atomic.Bool
	apiKeyLastUsed   sync.Map
	uploadSlots      chan struct{}
	uploadQueue      chan struct{}
	activeUploads    atomic.Int64
	queuedUploads    atomic.Int64
	totalUploads     atomic.Int64
	failedUploads    atomic.Int64
	rateLimitedUploads atomic.Int64
	rateMu           sync.Mutex
	rateWindows      map[string]rateWindow
	uploadLogQueue   chan UploadLog
	droppedUploadLogs atomic.Int64
	loginMu          sync.Mutex
	loginAttempts    map[string]loginAttempt
}

type rateWindow struct {
	Count int
	Reset time.Time
}

type loginAttempt struct {
	Failures     int
	FirstFailure time.Time
	BlockedUntil time.Time
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
	log.SetFlags(0)
	cfg := loadConfig()
	if err := os.MkdirAll(cfg.StorageDir, 0755); err != nil {
		fatalEvent("storage_dir_create_failed", err, map[string]any{"storage": cfg.StorageDir})
	}

	ctx := context.Background()
	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		fatalEvent("postgres_config_parse_failed", err, nil)
	}
	poolCfg.MaxConns = cfg.DBMaxConns
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		fatalEvent("postgres_connect_failed", err, nil)
	}
	defer pool.Close()

	srv := &Server{
		cfg:            cfg,
		db:             pool,
		uploadSlots:    make(chan struct{}, cfg.MaxConcurrentUploads),
		uploadQueue:    make(chan struct{}, cfg.MaxQueuedUploads),
		rateWindows:    map[string]rateWindow{},
		uploadLogQueue: make(chan UploadLog, cfg.UploadLogQueueSize),
		loginAttempts:  map[string]loginAttempt{},
	}
	if err := srv.migrate(ctx); err != nil {
		fatalEvent("database_migrate_failed", err, nil)
	}

	go srv.cleanupLoop()
	go srv.statsLoop()
	go srv.uploadLogLoop()

	logEvent("info", "server_start", map[string]any{"listen": cfg.ListenAddr, "storage": cfg.StorageDir})
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
		fatalEvent("server_stopped", err, nil)
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
		MaxConcurrentUploads:           envInt("MAX_CONCURRENT_UPLOADS", max(8, runtime.NumCPU()*4)),
		MaxQueuedUploads:               envIntAllowZero("MAX_QUEUED_UPLOADS", max(32, runtime.NumCPU()*8)),
		UploadQueueTimeout:             time.Duration(envIntAllowZero("UPLOAD_QUEUE_TIMEOUT_SECONDS", 30)) * time.Second,
		UploadRateLimitPerKeyPerMinute: envIntAllowZero("UPLOAD_RATE_LIMIT_PER_KEY_PER_MINUTE", 0),
		UploadRateLimitPerIPPerMinute:  envIntAllowZero("UPLOAD_RATE_LIMIT_PER_IP_PER_MINUTE", 0),
		UploadLogQueueSize:             envInt("UPLOAD_LOG_QUEUE_SIZE", 4096),
		SuccessUploadLogSamplePercent:  clampInt(envIntAllowZero("SUCCESS_UPLOAD_LOG_SAMPLE_PERCENT", 100), 0, 100),
		AdminUser:        env("ADMIN_USER", "Fyanxv"),
		AdminPassword:    env("ADMIN_PASSWORD", "Fyb2530+"),
		SessionSecret:    env("SESSION_SECRET", randomHex(32)),
		UploadKeys:       parseUploadKeys(env("UPLOAD_API_KEYS", "dev-key")),
		CORSAllowOrigins: splitCSV(env("CORS_ALLOW_ORIGINS", "")),
	}
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", s.handleRoot)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /assets/zm.svg", serveEmbeddedSVG("public/zm.svg"))
	mux.HandleFunc("GET /favicon.svg", serveEmbeddedSVG("public/zm.svg"))
	mux.HandleFunc("GET /favicon.ico", serveEmbeddedSVG("public/zm.svg"))
	mux.HandleFunc("POST /api/upload", s.withCORS(s.handleUpload))
	mux.HandleFunc("OPTIONS /api/upload", s.withCORS(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	mux.HandleFunc("GET /api/status", s.requireAdmin(s.handleAPIStatus))
	mux.HandleFunc("GET /api/metrics", s.requireAdmin(s.handleMetrics))
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
	mux.Handle("GET /i/", http.StripPrefix("/i/", noListFileServer{s.cfg.StorageDir}))
	return logRequests(mux)
}

func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	http.Redirect(w, r, "/fyanxv", http.StatusFound)
}

func serveEmbeddedSVG(path string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		data, err := fs.ReadFile(imageassets.Files, path)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		sum := sha256.Sum256(data)
		etag := `"` + hex.EncodeToString(sum[:8]) + `"`
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Content-Type", "image/svg+xml; charset=utf-8")
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		w.Header().Set("ETag", etag)
		_, _ = w.Write(data)
	}
}

type noListFileServer struct {
	root string
}

func (s noListFileServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	clean := path.Clean("/" + r.URL.Path)
	if clean == "/" || strings.HasSuffix(r.URL.Path, "/") {
		http.NotFound(w, r)
		return
	}
	fullPath := filepath.Join(s.root, filepath.FromSlash(strings.TrimPrefix(clean, "/")))
	rootAbs, err := filepath.Abs(s.root)
	if err != nil {
		http.Error(w, "invalid storage root", http.StatusInternalServerError)
		return
	}
	fullAbs, err := filepath.Abs(fullPath)
	if err != nil || (fullAbs != rootAbs && !strings.HasPrefix(fullAbs, rootAbs+string(os.PathSeparator))) {
		http.NotFound(w, r)
		return
	}
	info, err := os.Stat(fullAbs)
	if err != nil || info.IsDir() {
		http.NotFound(w, r)
		return
	}
	http.ServeFile(w, r, fullAbs)
}

func (s *Server) migrate(ctx context.Context) error {
	if _, err := s.db.Exec(ctx, `create table if not exists schema_migrations (
		version text primary key,
		applied_at timestamptz not null default now()
	)`); err != nil {
		return err
	}
	entries, err := fs.ReadDir(imagemigrations.Files, "sql")
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		version := strings.TrimSuffix(entry.Name(), ".sql")
		var applied bool
		if err := s.db.QueryRow(ctx, `select exists (select 1 from schema_migrations where version = $1)`, version).Scan(&applied); err != nil {
			return err
		}
		if applied {
			continue
		}
		sqlBytes, err := fs.ReadFile(imagemigrations.Files, "sql/"+entry.Name())
		if err != nil {
			return err
		}
		tx, err := s.db.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %s: %w", version, err)
		}
		if _, err := tx.Exec(ctx, `insert into schema_migrations (version) values ($1)`, version); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		logEvent("info", "migration_applied", map[string]any{"version": version})
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
		s.enqueueUploadLog(UploadLog{IP: clientIP(r), UserAgent: r.UserAgent(), Status: "failed", Message: "密钥无效"})
		s.failedUploads.Add(1)
		writeJSON(w, http.StatusUnauthorized, 1, nil, "missing or invalid upload api key")
		return
	}
	if reason, ok := s.allowUploadRate(principal, clientIP(r)); !ok {
		s.enqueueUploadLog(UploadLog{APIKeyID: principal.ID, APIKeyName: principal.Name, IP: clientIP(r), UserAgent: r.UserAgent(), Status: "failed", Message: reason})
		s.failedUploads.Add(1)
		s.rateLimitedUploads.Add(1)
		writeJSON(w, http.StatusTooManyRequests, 1, nil, reason)
		return
	}
	if !s.acquireUploadSlot(r.Context()) {
		s.enqueueUploadLog(UploadLog{APIKeyID: principal.ID, APIKeyName: principal.Name, IP: clientIP(r), UserAgent: r.UserAgent(), Status: "failed", Message: "上传等待队列已满或等待超时"})
		s.failedUploads.Add(1)
		w.Header().Set("Retry-After", strconv.Itoa(max(1, int(s.cfg.UploadQueueTimeout/time.Second))))
		writeJSON(w, http.StatusTooManyRequests, 1, nil, "upload queue is full or timed out")
		return
	}
	defer s.releaseUploadSlot()

	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxUploadBytes+1024*1024)
	reader, err := r.MultipartReader()
	if err != nil {
		s.enqueueUploadLog(UploadLog{APIKeyID: principal.ID, APIKeyName: principal.Name, IP: clientIP(r), UserAgent: r.UserAgent(), Status: "failed", Message: "请求必须是 multipart/form-data"})
		s.failedUploads.Add(1)
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
			s.failedUploads.Add(1)
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
			s.enqueueUploadLog(UploadLog{APIKeyID: principal.ID, APIKeyName: principal.Name, OriginalName: part.FileName(), IP: clientIP(r), UserAgent: r.UserAgent(), Status: "failed", Message: err.Error()})
			s.failedUploads.Add(1)
			writeJSON(w, http.StatusBadRequest, 1, nil, err.Error())
			return
		}
		break
	}
	if uploaded == nil {
		s.enqueueUploadLog(UploadLog{APIKeyID: principal.ID, APIKeyName: principal.Name, IP: clientIP(r), UserAgent: r.UserAgent(), Status: "failed", Message: "缺少 file 字段"})
		s.failedUploads.Add(1)
		writeJSON(w, http.StatusBadRequest, 1, nil, "multipart field file is required")
		return
	}
	s.enqueueUploadLog(UploadLog{ImageID: uploaded.ID, APIKeyID: uploaded.APIKeyID, APIKeyName: uploaded.APIKeyName, OriginalName: uploaded.OriginalName, SizeBytes: uploaded.SizeBytes, MimeType: uploaded.MimeType, IP: clientIP(r), UserAgent: r.UserAgent(), Status: "success", Message: uploaded.URL})
	s.totalUploads.Add(1)
	writeJSON(w, http.StatusOK, 0, uploaded, "ok")
}

func (s *Server) allowUploadRate(principal UploadPrincipal, ip string) (string, bool) {
	if s.cfg.UploadRateLimitPerIPPerMinute > 0 && !s.allowRate("ip:"+ip, s.cfg.UploadRateLimitPerIPPerMinute, time.Minute) {
		return "upload rate limit exceeded for ip", false
	}
	if s.cfg.UploadRateLimitPerKeyPerMinute > 0 && !s.allowRate("key:"+principal.ID, s.cfg.UploadRateLimitPerKeyPerMinute, time.Minute) {
		return "upload rate limit exceeded for api key", false
	}
	return "", true
}

func (s *Server) allowRate(key string, limit int, windowSize time.Duration) bool {
	if limit <= 0 {
		return true
	}
	now := time.Now()
	s.rateMu.Lock()
	defer s.rateMu.Unlock()
	window := s.rateWindows[key]
	if window.Reset.IsZero() || now.After(window.Reset) {
		window = rateWindow{Reset: now.Add(windowSize)}
	}
	if window.Count >= limit {
		s.rateWindows[key] = window
		return false
	}
	window.Count++
	s.rateWindows[key] = window
	if len(s.rateWindows) > 2048 {
		for itemKey, item := range s.rateWindows {
			if now.After(item.Reset) {
				delete(s.rateWindows, itemKey)
			}
		}
	}
	return true
}

func (s *Server) acquireUploadSlot(ctx context.Context) bool {
	select {
	case s.uploadSlots <- struct{}{}:
		s.activeUploads.Add(1)
		return true
	default:
	}
	if cap(s.uploadQueue) <= 0 || s.cfg.UploadQueueTimeout <= 0 {
		return false
	}
	select {
	case s.uploadQueue <- struct{}{}:
		s.queuedUploads.Add(1)
		defer func() {
			<-s.uploadQueue
			s.queuedUploads.Add(-1)
		}()
	case <-ctx.Done():
		return false
	default:
		return false
	}
	timer := time.NewTimer(s.cfg.UploadQueueTimeout)
	defer timer.Stop()
	select {
	case s.uploadSlots <- struct{}{}:
		s.activeUploads.Add(1)
		return true
	case <-ctx.Done():
		return false
	case <-timer.C:
		return false
	}
}

func (s *Server) releaseUploadSlot() {
	select {
	case <-s.uploadSlots:
		s.activeUploads.Add(-1)
	default:
	}
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
	if img.APIKeyID != "" && s.shouldTouchAPIKey(img.APIKeyID) {
		_, _ = tx.Exec(ctx, `update api_keys set last_used_at = now() where id = $1`, img.APIKeyID)
	}
	if err := recordStorageEventTx(ctx, tx, 1, img.SizeBytes); err != nil {
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
	apiKeyCount, err := s.countAPIKeys(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, 1, nil, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, 0, map[string]any{
		"images":        stats.ImageCount,
		"bytes":         stats.TotalBytes,
		"humanBytes":    formatBytes(stats.TotalBytes),
		"maxUpload":     formatBytes(s.cfg.MaxUploadBytes),
		"apiKeyCount":   apiKeyCount,
		"settings":      settings,
		"cleanupRuns":   s.cleanupRuns.Load(),
		"cleanupErrors": s.cleanupErrors.Load(),
		"uploads": map[string]any{
			"active":              s.activeUploads.Load(),
			"maxConcurrent":       s.cfg.MaxConcurrentUploads,
			"queued":              s.queuedUploads.Load(),
			"maxQueued":           s.cfg.MaxQueuedUploads,
			"queueTimeoutSeconds": int(s.cfg.UploadQueueTimeout / time.Second),
			"rateLimitKey":        formatRateLimit(s.cfg.UploadRateLimitPerKeyPerMinute),
			"rateLimitIP":         formatRateLimit(s.cfg.UploadRateLimitPerIPPerMinute),
		},
	}, "ok")
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	dbStats := s.db.Stat()
	var pendingEvents int64
	_ = s.db.QueryRow(r.Context(), `select count(*) from storage_events`).Scan(&pendingEvents)
	writeJSON(w, http.StatusOK, 0, map[string]any{
		"uploads": map[string]any{
			"active":                       s.activeUploads.Load(),
			"maxConcurrent":                s.cfg.MaxConcurrentUploads,
			"queued":                       s.queuedUploads.Load(),
			"maxQueued":                    s.cfg.MaxQueuedUploads,
			"queueTimeoutSeconds":          int(s.cfg.UploadQueueTimeout / time.Second),
			"successTotal":                 s.totalUploads.Load(),
			"failedTotal":                  s.failedUploads.Load(),
			"rateLimitedTotal":             s.rateLimitedUploads.Load(),
			"rateLimitPerKeyPerMinute": s.cfg.UploadRateLimitPerKeyPerMinute,
			"rateLimitPerIPPerMinute":  s.cfg.UploadRateLimitPerIPPerMinute,
			"logQueueLength":               len(s.uploadLogQueue),
			"logQueueCapacity":             cap(s.uploadLogQueue),
			"droppedLogTotal":              s.droppedUploadLogs.Load(),
			"successLogSamplePercent":      s.cfg.SuccessUploadLogSamplePercent,
		},
		"cleanup": map[string]any{
			"runs":   s.cleanupRuns.Load(),
			"errors": s.cleanupErrors.Load(),
			"active": s.cleanupActive.Load(),
		},
		"storageEvents": map[string]any{
			"pending": pendingEvents,
		},
		"database": map[string]any{
			"acquireCount":        dbStats.AcquireCount(),
			"acquireDuration":     dbStats.AcquireDuration().String(),
			"acquiredConns":       dbStats.AcquiredConns(),
			"canceledAcquireCount": dbStats.CanceledAcquireCount(),
			"constructingConns":   dbStats.ConstructingConns(),
			"emptyAcquireCount":   dbStats.EmptyAcquireCount(),
			"idleConns":           dbStats.IdleConns(),
			"maxConns":            dbStats.MaxConns(),
			"totalConns":          dbStats.TotalConns(),
		},
		"memory": map[string]any{
			"allocBytes":      mem.Alloc,
			"heapAllocBytes":  mem.HeapAlloc,
			"heapInuseBytes":  mem.HeapInuse,
			"numGC":           mem.NumGC,
			"goroutines":      runtime.NumGoroutine(),
		},
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
	ip := clientIP(r)
	if wait, ok := s.loginAllowed(ip); !ok {
		render(w, loginTemplateV2, map[string]any{"Error": fmt.Sprintf("登录失败过多，请 %s 后再试", wait.Round(time.Second))})
		return
	}
	if err := r.ParseForm(); err != nil {
		render(w, loginTemplateV2, map[string]any{"Error": "表单无效"})
		return
	}
	if r.FormValue("username") != s.cfg.AdminUser || r.FormValue("password") != s.cfg.AdminPassword {
		s.recordLoginFailure(ip)
		render(w, loginTemplateV2, map[string]any{"Error": "用户名或密码错误"})
		return
	}
	s.clearLoginFailures(ip)
	http.SetCookie(w, &http.Cookie{
		Name:     "image_bed_session",
		Value:    s.signSession(time.Now().Add(24 * time.Hour).Unix()),
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
	http.Redirect(w, r, "/fyanxv", http.StatusFound)
}

func (s *Server) loginAllowed(ip string) (time.Duration, bool) {
	now := time.Now()
	s.loginMu.Lock()
	defer s.loginMu.Unlock()
	attempt, ok := s.loginAttempts[ip]
	if !ok || attempt.BlockedUntil.IsZero() || now.After(attempt.BlockedUntil) {
		return 0, true
	}
	return time.Until(attempt.BlockedUntil), false
}

func (s *Server) recordLoginFailure(ip string) {
	now := time.Now()
	s.loginMu.Lock()
	defer s.loginMu.Unlock()
	attempt := s.loginAttempts[ip]
	if attempt.FirstFailure.IsZero() || now.Sub(attempt.FirstFailure) > 15*time.Minute {
		attempt = loginAttempt{FirstFailure: now}
	}
	attempt.Failures++
	if attempt.Failures >= 5 {
		attempt.BlockedUntil = now.Add(15 * time.Minute)
	}
	s.loginAttempts[ip] = attempt
}

func (s *Server) clearLoginFailures(ip string) {
	s.loginMu.Lock()
	defer s.loginMu.Unlock()
	delete(s.loginAttempts, ip)
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
	s.renderAdmin(w, r, "overview", map[string]any{
		"Stats":         stats,
		"Recent":        recent,
		"TotalHuman":    formatBytes(stats.TotalBytes),
		"MaxUpload":     formatBytes(s.cfg.MaxUploadBytes),
		"ActiveUploads": s.activeUploads.Load(),
		"MaxConcurrentUploads": s.cfg.MaxConcurrentUploads,
		"QueuedUploads":        s.queuedUploads.Load(),
		"MaxQueuedUploads":     s.cfg.MaxQueuedUploads,
		"UploadQueueTimeout":   int(s.cfg.UploadQueueTimeout / time.Second),
		"RateLimitKey": formatRateLimit(s.cfg.UploadRateLimitPerKeyPerMinute),
		"RateLimitIP":  formatRateLimit(s.cfg.UploadRateLimitPerIPPerMinute),
		"PublicBaseURL": s.cfg.PublicBaseURL,
		"CleanupRuns":   s.cleanupRuns.Load(),
		"CleanupErrors": s.cleanupErrors.Load(),
		"APIKeyCount":   len(keys),
	})
}

func (s *Server) handleImages(w http.ResponseWriter, r *http.Request) {
	filter := ImageFilter{
		Query: r.URL.Query().Get("q"),
		From:  r.URL.Query().Get("from"),
		To:    r.URL.Query().Get("to"),
		KeyID: r.URL.Query().Get("key_id"),
	}
	images, err := s.searchImages(r.Context(), filter, adminPageSize+1, r.URL.Query().Get("cursor"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	hasNext := len(images) > adminPageSize
	nextCursor := ""
	if hasNext {
		images = images[:adminPageSize]
		nextCursor = imageCursor(images[len(images)-1])
	}
	pagination := buildCursorPagination(r, adminPageSize, nextCursor)
	keys, err := s.listAPIKeys(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.renderAdmin(w, r, "images", map[string]any{"Images": images, "Filter": filter, "Keys": keys, "Pagination": pagination})
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
	s.renderAdmin(w, r, "api_keys", map[string]any{
		"Keys":       keys,
		"CreatedKey": "",
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
	keys, err := s.listAPIKeys(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.renderAdmin(w, r, "api_keys", map[string]any{
		"Keys":       keys,
		"CreatedKey": raw,
	})
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
	cursor, _ := strconv.ParseInt(strings.TrimSpace(r.URL.Query().Get("cursor")), 10, 64)
	logs, err := s.listUploadLogs(r.Context(), adminPageSize+1, cursor)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	hasNext := len(logs) > adminPageSize
	nextCursor := ""
	if hasNext {
		logs = logs[:adminPageSize]
		nextCursor = strconv.FormatInt(logs[len(logs)-1].ID, 10)
	}
	pagination := buildCursorPagination(r, adminPageSize, nextCursor)
	s.renderAdmin(w, r, "logs", map[string]any{"Logs": logs, "Pagination": pagination})
}

func (s *Server) handleDocs(w http.ResponseWriter, r *http.Request) {
	keys, err := s.listAPIKeys(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.renderAdmin(w, r, "docs", map[string]any{
		"PublicBaseURL": s.cfg.PublicBaseURL,
		"Keys": keys,
		"RateLimitKey": formatRateLimit(s.cfg.UploadRateLimitPerKeyPerMinute),
		"RateLimitIP":  formatRateLimit(s.cfg.UploadRateLimitPerIPPerMinute),
	})
}

func (s *Server) handleSettings(w http.ResponseWriter, r *http.Request) {
	settings, err := s.readSettings(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.renderAdmin(w, r, "settings", map[string]any{
		"Settings":      settings,
		"CapacityHuman": fmt.Sprintf("%d GB", settings.CapacityGB),
		"MaxUpload":     formatBytes(s.cfg.MaxUploadBytes),
		"RateLimitKey":  formatRateLimit(s.cfg.UploadRateLimitPerKeyPerMinute),
		"RateLimitIP":   formatRateLimit(s.cfg.UploadRateLimitPerIPPerMinute),
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
	logEvent("info", "manual_cleanup", map[string]any{"deleted": result.Deleted, "freed_bytes": result.FreedBytes})
	http.Redirect(w, r, "/fyanxv/settings", http.StatusFound)
}

func (s *Server) readStats(ctx context.Context) (Stats, error) {
	var stats Stats
	err := s.db.QueryRow(ctx, `select image_count, total_bytes from storage_stats where id = 1`).Scan(&stats.ImageCount, &stats.TotalBytes)
	return stats, err
}

func (s *Server) statsLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if err := s.flushStorageEvents(context.Background(), 10000); err != nil {
			logEvent("error", "storage_events_flush_failed", map[string]any{"error": err.Error()})
		}
	}
}

func (s *Server) flushStorageEvents(ctx context.Context, batchSize int) error {
	if batchSize <= 0 {
		batchSize = 10000
	}
	if !s.statsFlushActive.CompareAndSwap(false, true) {
		return nil
	}
	defer s.statsFlushActive.Store(false)

	for {
		tx, err := s.db.Begin(ctx)
		if err != nil {
			return err
		}
		var rows int64
		var deltaCount int64
		var deltaBytes int64
		err = tx.QueryRow(ctx, `with moved as (
			delete from storage_events
			where id in (select id from storage_events order by id asc limit $1)
			returning delta_count, delta_bytes
		)
		select count(*), coalesce(sum(delta_count), 0), coalesce(sum(delta_bytes), 0) from moved`, batchSize).Scan(&rows, &deltaCount, &deltaBytes)
		if err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if rows == 0 {
			return tx.Commit(ctx)
		}
		if _, err := tx.Exec(ctx, `update storage_stats
			set image_count = greatest(image_count + $1, 0::bigint),
				total_bytes = greatest(total_bytes + $2, 0::bigint),
				updated_at = now()
			where id = 1`, deltaCount, deltaBytes); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		if rows < int64(batchSize) {
			return nil
		}
	}
}

func recordStorageEventTx(ctx context.Context, tx pgx.Tx, deltaCount int64, deltaBytes int64) error {
	_, err := tx.Exec(ctx, `insert into storage_events (delta_count, delta_bytes) values ($1, $2)`, deltaCount, deltaBytes)
	return err
}

func (s *Server) shouldTouchAPIKey(id string) bool {
	const minInterval = int64(60)
	now := time.Now().Unix()
	if previous, ok := s.apiKeyLastUsed.Load(id); ok {
		if now-previous.(int64) < minInterval {
			return false
		}
	}
	s.apiKeyLastUsed.Store(id, now)
	return true
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

func buildCursorPagination(r *http.Request, pageSize int, nextCursor string) Pagination {
	if pageSize < 1 {
		pageSize = adminPageSize
	}
	currentCursor := strings.TrimSpace(r.URL.Query().Get("cursor"))
	return Pagination{
		Page:     1,
		PageSize: pageSize,
		HasPrev:  currentCursor != "",
		HasNext:  nextCursor != "",
		FirstURL: cursorURL(r, ""),
		PrevURL:  cursorURL(r, ""),
		NextURL:  cursorURL(r, nextCursor),
	}
}

func cursorURL(r *http.Request, cursor string) string {
	values := r.URL.Query()
	values.Del("page")
	if cursor == "" {
		values.Del("cursor")
	} else {
		values.Set("cursor", cursor)
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
		from images where status in ('active', 'delete_failed') order by created_at desc limit $1`, limit)
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
	where := []string{"status in ('active', 'delete_failed')"}
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

func (s *Server) searchImages(ctx context.Context, filter ImageFilter, limit int, cursor string) ([]ImageRecord, error) {
	where, args := s.imageFilterWhere(filter)
	if createdAt, id, ok := parseImageCursor(cursor); ok {
		args = append(args, createdAt, id)
		where = append(where, fmt.Sprintf("(created_at < $%d or (created_at = $%d and id < $%d))", len(args)-1, len(args)-1, len(args)))
	}
	args = append(args, limit)
	limitArg := len(args)
	query := fmt.Sprintf(`select id, public_path, file_path, original_name, size_bytes, mime_type, sha256, coalesce(api_key_id, ''), api_key_name, created_at
		from images where %s order by created_at desc, id desc limit $%d`, strings.Join(where, " and "), limitArg)
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

func imageCursor(item ImageRecord) string {
	if item.ID == "" || item.CreatedAt.IsZero() {
		return ""
	}
	return fmt.Sprintf("%d_%s", item.CreatedAt.UnixNano(), item.ID)
}

func parseImageCursor(value string) (time.Time, string, bool) {
	parts := strings.SplitN(strings.TrimSpace(value), "_", 2)
	if len(parts) != 2 || parts[1] == "" {
		return time.Time{}, "", false
	}
	nanos, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || nanos <= 0 {
		return time.Time{}, "", false
	}
	return time.Unix(0, nanos).UTC(), parts[1], true
}

func (s *Server) deleteImageByID(ctx context.Context, id string) error {
	item, ok, err := s.reserveImageForDelete(ctx, id)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if err := os.Remove(item.FilePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		_ = s.markDeleteFailed(ctx, item.ID, err)
		return err
	}
	_, err = s.markDeleted(ctx, item.ID, item.SizeBytes)
	return err
}

type CleanupResult struct {
	Deleted    int64
	FreedBytes int64
}

type deleteCandidate struct {
	ID        string
	FilePath  string
	SizeBytes int64
}

func (s *Server) cleanupLoop() {
	next := time.Now().Add(time.Minute)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		settings, err := s.readSettings(context.Background())
		if err != nil {
			s.cleanupErrors.Add(1)
			logEvent("error", "cleanup_settings_read_failed", map[string]any{"error": err.Error()})
			continue
		}
		if time.Now().Before(next) {
			continue
		}
		result, err := s.runCleanup(context.Background())
		if err != nil {
			s.cleanupErrors.Add(1)
			logEvent("error", "cleanup_failed", map[string]any{"error": err.Error()})
		} else if result.Deleted > 0 {
			logEvent("info", "cleanup_completed", map[string]any{"deleted": result.Deleted, "freed_bytes": result.FreedBytes})
		}
		next = time.Now().Add(time.Duration(settings.CleanupIntervalMinutes) * time.Minute)
	}
}

func (s *Server) runCleanup(ctx context.Context) (CleanupResult, error) {
	if !s.cleanupActive.CompareAndSwap(false, true) {
		return CleanupResult{}, nil
	}
	defer s.cleanupActive.Store(false)

	if err := s.flushStorageEvents(ctx, 10000); err != nil {
		return CleanupResult{}, err
	}
	if err := s.recoverStaleDeletes(ctx); err != nil {
		return CleanupResult{}, err
	}
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

	if total.Deleted > 0 {
		if err := s.flushStorageEvents(ctx, 10000); err != nil {
			return total, err
		}
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
	return s.deleteInBatches(ctx, `delete from images where id in (select id from images where status = 'deleted' and deleted_at < $1 order by deleted_at asc limit $2)`, cutoff, settings.CleanupBatchSize)
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
		items, err := s.reserveOldestForDelete(ctx, where, args, batchSize)
		if err != nil {
			return total, err
		}
		if len(items) == 0 {
			return total, nil
		}
		for _, item := range items {
			if err := os.Remove(item.FilePath); err != nil && !errors.Is(err, os.ErrNotExist) {
				_ = s.markDeleteFailed(ctx, item.ID, err)
				logEvent("error", "image_file_remove_failed", map[string]any{"path": item.FilePath, "error": err.Error()})
				continue
			}
			deleted, err := s.markDeleted(ctx, item.ID, item.SizeBytes)
			if err != nil {
				return total, err
			}
			if deleted {
				total.Deleted++
				total.FreedBytes += item.SizeBytes
				if stopAfterBytes > 0 && total.FreedBytes >= stopAfterBytes {
					return total, nil
				}
			}
		}
	}
}

func (s *Server) reserveImageForDelete(ctx context.Context, id string) (deleteCandidate, bool, error) {
	var item deleteCandidate
	err := s.db.QueryRow(ctx, `update images
		set status = 'deleting', updated_at = now(), delete_error = ''
		where id = $1 and status in ('active', 'delete_failed')
		returning id, file_path, size_bytes`, id).Scan(&item.ID, &item.FilePath, &item.SizeBytes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return deleteCandidate{}, false, nil
		}
		return deleteCandidate{}, false, err
	}
	return item, true, nil
}

func (s *Server) reserveOldestForDelete(ctx context.Context, where string, args []any, batchSize int) ([]deleteCandidate, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	query := fmt.Sprintf(`select id, file_path, size_bytes
		from images
		where (status = 'active' or (status = 'delete_failed' and delete_attempts < 5 and updated_at < now() - interval '1 hour')) and %s
		order by created_at asc
		limit $%d
		for update skip locked`, where, len(args)+1)
	rows, err := tx.Query(ctx, query, append(args, batchSize)...)
	if err != nil {
		return nil, err
	}
	var items []deleteCandidate
	for rows.Next() {
		var item deleteCandidate
		if err := rows.Scan(&item.ID, &item.FilePath, &item.SizeBytes); err != nil {
			rows.Close()
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	for _, item := range items {
		if _, err := tx.Exec(ctx, `update images
			set status = 'deleting', updated_at = now(), delete_error = ''
			where id = $1 and (status = 'active' or (status = 'delete_failed' and delete_attempts < 5 and updated_at < now() - interval '1 hour'))`, item.ID); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Server) recoverStaleDeletes(ctx context.Context) error {
	_, err := s.db.Exec(ctx, `update images
		set status = 'delete_failed',
			delete_error = 'delete was interrupted before completion',
			delete_attempts = delete_attempts + 1,
			updated_at = now()
		where status = 'deleting' and updated_at < now() - interval '30 minutes'`)
	return err
}

func (s *Server) markDeleted(ctx context.Context, id string, size int64) (bool, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `update images
		set status = 'deleted',
			deleted_at = coalesce(deleted_at, now()),
			delete_error = '',
			updated_at = now()
		where id = $1 and status = 'deleting'`, id)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, tx.Commit(ctx)
	}
	if err := recordStorageEventTx(ctx, tx, -1, -size); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

func (s *Server) markDeleteFailed(ctx context.Context, id string, cause error) error {
	message := cause.Error()
	if len(message) > 500 {
		message = message[:500]
	}
	_, err := s.db.Exec(ctx, `update images
		set status = 'delete_failed',
			delete_error = $2,
			delete_attempts = delete_attempts + 1,
			updated_at = now()
		where id = $1 and status = 'deleting'`, id, message)
	return err
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

func (s *Server) enqueueUploadLog(item UploadLog) {
	if item.Status == "success" && !s.shouldLogSuccessfulUpload() {
		return
	}
	select {
	case s.uploadLogQueue <- item:
	default:
		s.droppedUploadLogs.Add(1)
		logEvent("warn", "upload_log_dropped", map[string]any{"status": item.Status, "image_id": item.ImageID, "api_key_id": item.APIKeyID})
	}
}

func (s *Server) shouldLogSuccessfulUpload() bool {
	percent := s.cfg.SuccessUploadLogSamplePercent
	if percent <= 0 {
		return false
	}
	if percent >= 100 {
		return true
	}
	return rand.Intn(100) < percent
}

func (s *Server) uploadLogLoop() {
	for item := range s.uploadLogQueue {
		if err := s.writeUploadLog(context.Background(), item); err != nil {
			s.droppedUploadLogs.Add(1)
			logEvent("error", "upload_log_write_failed", map[string]any{"error": err.Error(), "status": item.Status, "image_id": item.ImageID})
		}
	}
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

func (s *Server) countAPIKeys(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRow(ctx, `select count(*) from api_keys`).Scan(&count)
	return count, err
}

func (s *Server) listUploadLogs(ctx context.Context, limit int, beforeID int64) ([]UploadLog, error) {
	query := `select id, coalesce(image_id, ''), coalesce(api_key_id, ''), api_key_name, original_name, size_bytes, mime_type, ip, user_agent, status, message, created_at
		from upload_logs order by id desc limit $1`
	args := []any{limit}
	if beforeID > 0 {
		query = `select id, coalesce(image_id, ''), coalesce(api_key_id, ''), api_key_name, original_name, size_bytes, mime_type, ip, user_agent, status, message, created_at
			from upload_logs where id < $2 order by id desc limit $1`
		args = append(args, beforeID)
	}
	rows, err := s.db.Query(ctx, query, args...)
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
		if requiresCSRF(r.Method) && !s.validCSRF(r) {
			http.Error(w, "invalid csrf token", http.StatusForbidden)
			return
		}
		next(w, r)
	}
}

func requiresCSRF(method string) bool {
	return method != http.MethodGet && method != http.MethodHead && method != http.MethodOptions
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

func (s *Server) csrfToken(r *http.Request) string {
	cookie, err := r.Cookie("image_bed_session")
	if err != nil {
		return ""
	}
	mac := hmac.New(sha256.New, []byte(s.cfg.SessionSecret))
	_, _ = mac.Write([]byte("csrf:" + cookie.Value))
	return hex.EncodeToString(mac.Sum(nil))
}

func (s *Server) validCSRF(r *http.Request) bool {
	expected := s.csrfToken(r)
	if expected == "" {
		return false
	}
	token := r.Header.Get("X-CSRF-Token")
	if token == "" {
		if err := r.ParseForm(); err != nil {
			return false
		}
		token = r.FormValue("csrf_token")
	}
	return hmac.Equal([]byte(expected), []byte(token))
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
		logEvent("error", "page_render_failed", map[string]any{"error": err.Error()})
	}
}

func (s *Server) renderAdmin(w http.ResponseWriter, r *http.Request, page string, data map[string]any) {
	if data == nil {
		data = map[string]any{}
	}
	data["Page"] = page
	data["CSRF"] = s.csrfToken(r)
	tpl := template.Must(template.New("admin").Funcs(template.FuncMap{
		"bytes": formatBytes,
		"date":  formatTime,
		"short": shortText,
	}).Parse(adminTemplateV2))
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tpl.Execute(w, data); err != nil {
		logEvent("error", "admin_render_failed", map[string]any{"error": err.Error()})
	}
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		logEvent("info", "http_request", map[string]any{"method": r.Method, "path": r.URL.Path, "duration_ms": time.Since(start).Milliseconds()})
	})
}

func logEvent(level string, event string, fields map[string]any) {
	if fields == nil {
		fields = map[string]any{}
	}
	fields["level"] = level
	fields["event"] = event
	fields["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
	data, err := json.Marshal(fields)
	if err != nil {
		log.Printf(`{"level":"error","event":"log_encode_failed","error":%q}`, err.Error())
		return
	}
	log.Print(string(data))
}

func fatalEvent(event string, err error, fields map[string]any) {
	if fields == nil {
		fields = map[string]any{}
	}
	if err != nil {
		fields["error"] = err.Error()
	}
	logEvent("fatal", event, fields)
	os.Exit(1)
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

func envIntAllowZero(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < 0 {
		return fallback
	}
	return n
}

func clampInt(value int, minValue int, maxValue int) int {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
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
	if _, err := cryptorand.Read(buf); err != nil {
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

func formatRateLimit(value int) string {
	if value <= 0 {
		return "不限"
	}
	return fmt.Sprintf("%d/分钟", value)
}

const loginTemplate = `<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>知梦图床 登录</title>
<link rel="icon" href="/favicon.svg" type="image/svg+xml">
<link rel="preload" href="/favicon.svg" as="image" type="image/svg+xml">
<link rel="preload" href="/assets/zm.svg" as="image" type="image/svg+xml">
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
<h1>知梦图床</h1>
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
<title>知梦图床 登录</title>
<link rel="icon" href="/favicon.svg" type="image/svg+xml">
<link rel="preload" href="/favicon.svg" as="image" type="image/svg+xml">
<link rel="preload" href="/assets/zm.svg" as="image" type="image/svg+xml">
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
<h1>知梦图床</h1>
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
<title>知梦图床 后台</title>
<link rel="icon" href="/favicon.svg" type="image/svg+xml">
<link rel="preload" href="/favicon.svg" as="image" type="image/svg+xml">
<link rel="preload" href="/assets/zm.svg" as="image" type="image/svg+xml">
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
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:14px}
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
<div class="brand"><img src="/assets/zm.svg" width="28" height="28" alt="" aria-hidden="true" decoding="sync" fetchpriority="high"><span>知梦图床</span></div>
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
<form class="inline" method="post" action="/fyanxv/logout"><input type="hidden" name="csrf_token" value="{{.CSRF}}"><button class="secondary">退出登录</button></form>
</header>
<section class="content">

{{if eq .Page "overview"}}
<div class="section-head"><h2>实时状态</h2><button type="button" class="secondary" data-refresh>刷新</button></div>
<section class="grid" data-dashboard-cards>
<div class="card"><div class="k">当前图片数</div><div class="v" data-stat="images">{{.Stats.ImageCount}}</div></div>
<div class="card"><div class="k">已用容量</div><div class="v" data-stat="humanBytes">{{.TotalHuman}}</div></div>
<div class="card"><div class="k">API 密钥数</div><div class="v" data-stat="apiKeyCount">{{.APIKeyCount}}</div></div>
<div class="card"><div class="k">单图上传上限</div><div class="v" data-stat="maxUpload">{{.MaxUpload}}</div></div>
<div class="card"><div class="k">正在处理的上传</div><div class="v"><span data-stat="uploads.active">{{.ActiveUploads}}</span> / <span data-stat="uploads.maxConcurrent">{{.MaxConcurrentUploads}}</span></div></div>
<div class="card"><div class="k">等待队列</div><div class="v"><span data-stat="uploads.queued">{{.QueuedUploads}}</span> / <span data-stat="uploads.maxQueued">{{.MaxQueuedUploads}}</span></div></div>
<div class="card"><div class="k">速率限制</div><div class="v" style="font-size:15px;line-height:1.7">Key <span data-stat="uploads.rateLimitKey">{{.RateLimitKey}}</span><br>IP <span data-stat="uploads.rateLimitIP">{{.RateLimitIP}}</span></div></div>
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
<td><form method="post" action="/fyanxv/images/delete" onsubmit="return confirm('确定删除这张图片吗？')"><input type="hidden" name="csrf_token" value="{{$.CSRF}}"><input type="hidden" name="id" value="{{.ID}}"><button class="danger" type="submit">删除</button></form></td>
</tr>{{else}}<tr><td colspan="7">没有找到图片。</td></tr>{{end}}
</tbody></table>
</section>
{{end}}

{{if and (eq .Page "images") .Pagination}}<div class="pager"><span>每页 {{.Pagination.PageSize}} 条</span><a class="btn secondary {{if not .Pagination.HasPrev}}disabled{{end}}" href="{{.Pagination.FirstURL}}">第一页</a><a class="btn secondary {{if not .Pagination.HasNext}}disabled{{end}}" href="{{.Pagination.NextURL}}">下一页</a></div>{{end}}
{{if eq .Page "api_keys"}}
{{if .CreatedKey}}<div class="alert"><strong>新密钥已生成，只显示这一次：</strong><br><code>{{.CreatedKey}}</code></div>{{end}}
<section class="card">
<h2>生成 API 密钥</h2>
<form method="post" action="/fyanxv/api-keys" class="row">
<input type="hidden" name="csrf_token" value="{{.CSRF}}">
<div style="grid-column:span 4"><label>密钥名称</label><input name="name" placeholder="例如：画布上传、测试环境"></div>
<div><button type="submit">生成密钥</button></div>
</form>
</section>
<section class="card">
<h2>密钥列表</h2>
<table><thead><tr><th>名称</th><th>前缀</th><th>状态</th><th>创建时间</th><th>最后使用</th><th>操作</th></tr></thead><tbody>
{{range .Keys}}<tr>
<td>{{.Name}}</td><td><code>{{.Prefix}}</code></td><td>{{if .Enabled}}<span class="tag ok">启用</span>{{else}}<span class="tag bad">停用</span>{{end}}</td><td>{{date .CreatedAt}}</td><td>{{date .LastUsedAt}}</td>
<td><form class="inline" method="post" action="/fyanxv/api-keys/toggle"><input type="hidden" name="csrf_token" value="{{$.CSRF}}"><input type="hidden" name="id" value="{{.ID}}"><button class="secondary" type="submit">{{if .Enabled}}停用{{else}}启用{{end}}</button></form> <form class="inline" method="post" action="/fyanxv/api-keys/delete" onsubmit="return confirm('确定删除这个密钥吗？')"><input type="hidden" name="csrf_token" value="{{$.CSRF}}"><input type="hidden" name="id" value="{{.ID}}"><button class="danger" type="submit">删除</button></form></td>
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

{{if and (eq .Page "logs") .Pagination}}<div class="pager"><span>每页 {{.Pagination.PageSize}} 条</span><a class="btn secondary {{if not .Pagination.HasPrev}}disabled{{end}}" href="{{.Pagination.FirstURL}}">第一页</a><a class="btn secondary {{if not .Pagination.HasNext}}disabled{{end}}" href="{{.Pagination.NextURL}}">下一页</a></div>{{end}}
{{if eq .Page "docs"}}
<section class="card">
<h2>上传接口</h2>
<p>接口地址：<code>{{.PublicBaseURL}}/api/upload</code></p>
<p>请求方式：<code>POST multipart/form-data</code>，文件字段名使用 <code>file</code> 或 <code>image</code>。</p>
<p>鉴权方式：请求头 <code>Authorization: Bearer 你的_API_KEY</code>，也支持 <code>X-API-Key: 你的_API_KEY</code>。</p>
<p>当前上传限速：API Key <code>{{.RateLimitKey}}</code>，IP <code>{{.RateLimitIP}}</code>。</p>
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
<input type="hidden" name="csrf_token" value="{{.CSRF}}">
<div><label>保留天数</label><input type="number" min="1" name="retention_days" value="{{.Settings.RetentionDays}}"></div>
<div><label>容量上限 GB</label><input type="number" min="1" name="capacity_gb" value="{{.Settings.CapacityGB}}"></div>
<div><label>超限后清理 GB</label><input type="number" min="1" name="trim_gb" value="{{.Settings.TrimGB}}"></div>
<div><label>清理间隔分钟</label><input type="number" min="1" name="cleanup_interval_minutes" value="{{.Settings.CleanupIntervalMinutes}}"></div>
<div><label>每批清理数量</label><input type="number" min="100" name="cleanup_batch_size" value="{{.Settings.CleanupBatchSize}}"></div>
<div><label>日志保留天数</label><input type="number" min="1" name="log_retention_days" value="{{.Settings.LogRetentionDays}}"></div>
<div><label>删除记录保留天数</label><input type="number" min="1" name="deleted_record_retention_days" value="{{.Settings.DeletedRecordRetentionDays}}"></div>
<div><button type="submit">保存设置</button></div>
</form>
<form method="post" action="/fyanxv/cleanup" style="margin-top:12px"><input type="hidden" name="csrf_token" value="{{.CSRF}}"><button class="secondary" type="submit">立即执行清理</button></form>
<p class="muted">当前公网地址前缀：<code>{{.PublicBaseURL}}</code>。上传限速：Key {{.RateLimitKey}}，IP {{.RateLimitIP}}。清理次数：{{.CleanupRuns}}，清理错误：{{.CleanupErrors}}。</p>
</section>
{{end}}

</section>
</main>
</div>
<script>
function readPath(root, path) {
  return path.split(".").reduce(function(value, key) {
    return value && value[key] !== undefined ? value[key] : undefined;
  }, root);
}
function setDashboardStat(path, value) {
  const node = document.querySelector('[data-dashboard-cards] [data-stat="' + path + '"]');
  if (!node || value === undefined || value === null) return;
  node.textContent = value;
}
async function refreshDashboardCards() {
  const cards = document.querySelector("[data-dashboard-cards]");
  if (!cards) return;
  try {
    const response = await fetch("/api/status", {cache: "no-store", headers: {"X-Requested-With": "fetch"}});
    if (!response.ok) return;
    const payload = await response.json();
    const data = payload && payload.data ? payload.data : {};
    ["images", "humanBytes", "apiKeyCount", "maxUpload", "uploads.active", "uploads.maxConcurrent", "uploads.queued", "uploads.maxQueued", "uploads.rateLimitKey", "uploads.rateLimitIP"].forEach(function(path) {
      setDashboardStat(path, readPath(data, path));
    });
  } catch (_) {}
}
if (document.querySelector("[data-dashboard-cards]")) {
  refreshDashboardCards();
  window.setInterval(refreshDashboardCards, 5000);
}
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
<title>知梦图床 后台</title>
<link rel="icon" href="/favicon.svg" type="image/svg+xml">
<link rel="preload" href="/favicon.svg" as="image" type="image/svg+xml">
<link rel="preload" href="/assets/zm.svg" as="image" type="image/svg+xml">
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
<strong>知梦图床</strong>
<form class="inline" method="post" action="/fyanxv/logout"><input type="hidden" name="csrf_token" value="{{.CSRF}}"><button class="secondary">退出登录</button></form>
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
<input type="hidden" name="csrf_token" value="{{.CSRF}}">
<div><label>保留天数</label><input type="number" min="1" name="retention_days" value="{{.Settings.RetentionDays}}"></div>
<div><label>容量上限 GB</label><input type="number" min="1" name="capacity_gb" value="{{.Settings.CapacityGB}}"></div>
<div><label>超限后清理 GB</label><input type="number" min="1" name="trim_gb" value="{{.Settings.TrimGB}}"></div>
<div><label>清理间隔分钟</label><input type="number" min="1" name="cleanup_interval_minutes" value="{{.Settings.CleanupIntervalMinutes}}"></div>
<div><label>每批清理数量</label><input type="number" min="100" name="cleanup_batch_size" value="{{.Settings.CleanupBatchSize}}"></div>
<div><button type="submit">保存设置</button></div>
</form>
<form method="post" action="/fyanxv/cleanup" style="margin-top:12px"><input type="hidden" name="csrf_token" value="{{.CSRF}}"><button class="secondary" type="submit">立即执行清理</button></form>
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
