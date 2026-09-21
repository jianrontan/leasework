// Package migrations embeds leasework's Postgres schema migrations so the
// binary carries its own schema and does not depend on files present on the
// deployment host.
package migrations

import "embed"

// FS holds the embedded goose migration files.
//
//go:embed *.sql
var FS embed.FS
