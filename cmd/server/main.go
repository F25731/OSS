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
	APIKeyName   string    `json:"apiKeyName"`
	CreatedAt    time.Time `json:"createdAt"`
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
	if cfg.AdminPassword == "admin123" {
		log.Printf("WARNING: ADMIN_PASSWORD is using the default value")
	}
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
		AdminUser:        env("ADMIN_USER", "admin"),
		AdminPassword:    env("ADMIN_PASSWORD", "admin123"),
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
	mux.HandleFunc("GET /admin/login", s.handleLoginPage)
	mux.HandleFunc("POST /admin/login", s.handleLogin)
	mux.HandleFunc("POST /admin/logout", s.requireAdmin(s.handleLogout))
	mux.HandleFunc("GET /admin", s.requireAdmin(s.handleAdmin))
	mux.HandleFunc("POST /admin/settings", s.requireAdmin(s.handleSettingsUpdate))
	mux.HandleFunc("POST /admin/cleanup", s.requireAdmin(s.handleCleanupNow))
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
			api_key_name text not null,
			created_at timestamptz not null default now(),
			deleted_at timestamptz
		)`,
		`create index if not exists images_active_created_at_idx on images (created_at) where deleted_at is null`,
		`create index if not exists images_sha256_idx on images (sha256)`,
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
	}
	for key, value := range defaults {
		if _, err := s.db.Exec(ctx, `insert into settings (key, value) values ($1, $2) on conflict (key) do nothing`, key, value); err != nil {
			return err
		}
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
	keyName, ok := s.checkUploadAuth(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, 1, nil, "missing or invalid upload api key")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxUploadBytes+1024*1024)
	reader, err := r.MultipartReader()
	if err != nil {
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
		uploaded, err = s.saveMultipartImage(r.Context(), part, keyName)
		_ = part.Close()
		if err != nil {
			writeJSON(w, http.StatusBadRequest, 1, nil, err.Error())
			return
		}
		break
	}
	if uploaded == nil {
		writeJSON(w, http.StatusBadRequest, 1, nil, "multipart field file is required")
		return
	}
	writeJSON(w, http.StatusOK, 0, uploaded, "ok")
}

func (s *Server) saveMultipartImage(ctx context.Context, part *multipart.Part, keyName string) (*ImageRecord, error) {
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
		APIKeyName:   keyName,
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
		(id, public_path, file_path, original_name, size_bytes, mime_type, sha256, api_key_name, created_at)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		img.ID, img.PublicPath, img.FilePath, img.OriginalName, img.SizeBytes, img.MimeType, img.SHA256, img.APIKeyName, img.CreatedAt)
	if err != nil {
		return err
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
		http.Redirect(w, r, "/admin", http.StatusFound)
		return
	}
	render(w, loginTemplate, map[string]any{"Error": ""})
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		render(w, loginTemplate, map[string]any{"Error": "Invalid form"})
		return
	}
	if r.FormValue("username") != s.cfg.AdminUser || r.FormValue("password") != s.cfg.AdminPassword {
		render(w, loginTemplate, map[string]any{"Error": "Invalid username or password"})
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     "image_bed_session",
		Value:    s.signSession(time.Now().Add(24 * time.Hour).Unix()),
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
	http.Redirect(w, r, "/admin", http.StatusFound)
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{Name: "image_bed_session", Value: "", Path: "/", MaxAge: -1})
	http.Redirect(w, r, "/admin/login", http.StatusFound)
}

func (s *Server) handleAdmin(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	stats, err := s.readStats(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	settings, err := s.readSettings(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	recent, err := s.recentImages(ctx, 20)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	render(w, adminTemplate, map[string]any{
		"Stats":         stats,
		"Settings":      settings,
		"Recent":        recent,
		"TotalHuman":    formatBytes(stats.TotalBytes),
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
	}
	if err := s.saveSettings(r.Context(), settings); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/admin", http.StatusFound)
}

func (s *Server) handleCleanupNow(w http.ResponseWriter, r *http.Request) {
	result, err := s.runCleanup(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("manual cleanup: deleted=%d freed=%s", result.Deleted, formatBytes(result.FreedBytes))
	http.Redirect(w, r, "/admin", http.StatusFound)
}

func (s *Server) readStats(ctx context.Context) (Stats, error) {
	var stats Stats
	err := s.db.QueryRow(ctx, `select image_count, total_bytes from storage_stats where id = 1`).Scan(&stats.ImageCount, &stats.TotalBytes)
	return stats, err
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
	}, rows.Err()
}

func (s *Server) saveSettings(ctx context.Context, settings Settings) error {
	values := map[string]int{
		"retention_days":           settings.RetentionDays,
		"capacity_gb":              settings.CapacityGB,
		"trim_gb":                  settings.TrimGB,
		"cleanup_interval_minutes": settings.CleanupIntervalMinutes,
		"cleanup_batch_size":       settings.CleanupBatchSize,
	}
	for key, value := range values {
		if _, err := s.db.Exec(ctx, `insert into settings (key, value) values ($1, $2) on conflict (key) do update set value = excluded.value`, key, strconv.Itoa(value)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) recentImages(ctx context.Context, limit int) ([]ImageRecord, error) {
	rows, err := s.db.Query(ctx, `select id, public_path, file_path, original_name, size_bytes, mime_type, sha256, api_key_name, created_at
		from images where deleted_at is null order by created_at desc limit $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ImageRecord
	for rows.Next() {
		var item ImageRecord
		if err := rows.Scan(&item.ID, &item.PublicPath, &item.FilePath, &item.OriginalName, &item.SizeBytes, &item.MimeType, &item.SHA256, &item.APIKeyName, &item.CreatedAt); err != nil {
			return nil, err
		}
		item.URL = s.cfg.PublicBaseURL + item.PublicPath
		items = append(items, item)
	}
	return items, rows.Err()
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
	s.cleanupRuns.Add(1)
	return total, nil
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

func (s *Server) checkUploadAuth(r *http.Request) (string, bool) {
	key := strings.TrimSpace(r.Header.Get("X-API-Key"))
	if key == "" {
		auth := r.Header.Get("Authorization")
		if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
			key = strings.TrimSpace(auth[7:])
		}
	}
	name, ok := s.cfg.UploadKeys[key]
	return name, ok
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
			http.Redirect(w, r, "/admin/login", http.StatusFound)
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

const loginTemplate = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Image Bed Login</title>
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
<form class="box" method="post" action="/admin/login">
<h1>Image Bed Admin</h1>
{{if .Error}}<div class="err">{{.Error}}</div>{{end}}
<label>Username</label><input name="username" autocomplete="username" required>
<label>Password</label><input type="password" name="password" autocomplete="current-password" required>
<button type="submit">Login</button>
</form>
</body>
</html>`

const adminTemplate = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Image Bed Admin</title>
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
<strong>AI Image Bed</strong>
<form class="inline" method="post" action="/admin/logout"><button class="secondary">Logout</button></form>
</header>
<main>
<section class="grid">
<div class="card"><div class="k">Active images</div><div class="v">{{.Stats.ImageCount}}</div></div>
<div class="card"><div class="k">Storage used</div><div class="v">{{.TotalHuman}}</div></div>
<div class="card"><div class="k">Capacity</div><div class="v">{{.CapacityHuman}}</div></div>
<div class="card"><div class="k">Max upload</div><div class="v">{{.MaxUpload}}</div></div>
</section>

<section class="card" style="margin-top:16px">
<h2>Cleanup Settings</h2>
<form method="post" action="/admin/settings" class="settings">
<div><label>Retention days</label><input type="number" min="1" name="retention_days" value="{{.Settings.RetentionDays}}"></div>
<div><label>Capacity GB</label><input type="number" min="1" name="capacity_gb" value="{{.Settings.CapacityGB}}"></div>
<div><label>Trim GB after overflow</label><input type="number" min="1" name="trim_gb" value="{{.Settings.TrimGB}}"></div>
<div><label>Cleanup interval minutes</label><input type="number" min="1" name="cleanup_interval_minutes" value="{{.Settings.CleanupIntervalMinutes}}"></div>
<div><label>Batch size</label><input type="number" min="100" name="cleanup_batch_size" value="{{.Settings.CleanupBatchSize}}"></div>
<div><button type="submit">Save Settings</button></div>
</form>
<form method="post" action="/admin/cleanup" style="margin-top:12px"><button class="secondary" type="submit">Run Cleanup Now</button></form>
<p class="k">Cleanup runs: {{.CleanupRuns}}, errors: {{.CleanupErrors}}. Public base URL: <code>{{.PublicBaseURL}}</code></p>
</section>

<section class="card" style="margin-top:16px">
<h2>Recent Images</h2>
<table>
<thead><tr><th>Preview</th><th>URL</th><th>Size</th><th>MIME</th><th>API key</th><th>Created</th></tr></thead>
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
{{else}}<tr><td colspan="6">No images yet.</td></tr>{{end}}
</tbody>
</table>
</section>
</main>
</body>
</html>`
