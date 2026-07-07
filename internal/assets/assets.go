package assets

import "embed"

// Files contains static UI assets embedded into the server binary.
//go:embed public/*
var Files embed.FS
