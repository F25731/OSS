# AI Image Bed

A small self-hosted image bed for AI reference images.

It is designed for this flow:

1. Canvas uploads reference images through an API.
2. The service streams each image to local disk.
3. PostgreSQL stores only metadata.
4. The API returns a public image URL.
5. Nginx can serve `/i/*` directly as static files.

## Features

- `POST /api/upload` multipart upload with API key auth.
- Streaming upload, no base64 payloads.
- Local disk storage with date-based paths.
- PostgreSQL metadata and fast aggregate stats.
- Admin pages with overview, image search, single image deletion, API key
  management, upload logs, integration docs and cleanup settings.
- Database-backed upload API keys. New keys can be generated from the admin UI.
- Default cleanup: delete images older than 7 days.
- Default capacity policy: 100 GB cap, trim to 70 GB after overflow.
- Simple CORS support for browser uploads from your canvas domain.

## Quick Start

```bash
cp .env.example .env
docker compose up -d --build
```

Open:

```text
http://localhost:8080/fyanxv
```

Upload:

```bash
curl -X POST http://localhost:8080/api/upload \
  -H "Authorization: Bearer change-this-upload-key" \
  -F "file=@./example.png"
```

Response:

```json
{
  "code": 0,
  "data": {
    "id": "...",
    "publicPath": "/i/2026/07/05/....png",
    "url": "https://tc.zmoapi.cn/i/2026/07/05/....png",
    "sizeBytes": 12345
  },
  "msg": "ok"
}
```

## Environment

| Name | Default | Notes |
| --- | --- | --- |
| `LISTEN_ADDR` | `:8080` | Go service listen address. |
| `DATABASE_URL` | local postgres URL | PostgreSQL connection string. |
| `DB_MAX_CONNS` | CPU-based, at least 50 | PostgreSQL connection pool size. |
| `STORAGE_DIR` | `./data/images` | Image files are stored here. |
| `PUBLIC_BASE_URL` | `http://localhost:8080` | Prefix used for returned image URLs. |
| `MAX_UPLOAD_MB` | `50` | Single image upload limit. |
| `ADMIN_USER` | `Fyanxv` | Admin login username. |
| `ADMIN_PASSWORD` | `Fyb2530+` | Change in production. |
| `SESSION_SECRET` | random per boot | Set a stable random string in production. |
| `UPLOAD_API_KEYS` | `dev-key` | Format: `key:name,key2:name2`. |
| `CORS_ALLOW_ORIGINS` | empty | Comma-separated origins, or `*`. |

## Production Notes

- Put Nginx in front of the Go service.
- Let Nginx serve `/i/` directly from `STORAGE_DIR`.
- Keep upload API behind HTTPS.
- For non-Docker deployment, use `deploy/image-bed.service` as a systemd
  starting point.
- Raise Linux limits for high concurrency:

```bash
ulimit -n 1048576
```

- Tune Nginx:

```nginx
worker_processes auto;
worker_rlimit_nofile 1048576;
events { worker_connections 65535; multi_accept on; }
```

- PostgreSQL does not store image bytes, so it is not the main bottleneck.
  The likely bottlenecks are bandwidth, disk write throughput and file
  descriptor limits.
