package migrations

import "embed"

// Files contains versioned PostgreSQL migrations embedded into the server binary.
//go:embed sql/*.sql
var Files embed.FS
