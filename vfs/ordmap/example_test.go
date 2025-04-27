package ordmap_test

import (
	"database/sql"
	_ "embed"
	"testing"

	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
	"github.com/ncruces/go-sqlite3/vfs/ordmap"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/test.db
var testDB []byte

func TestExample(t *testing.T) {
	ordmap.Create("test.db", testDB)

	db := assert(sql.Open("sqlite3", "file:/test.db?vfs=ordmap")).noErr(t)
	defer db.Close()

	assert(db.Exec(`INSERT INTO users (id, name) VALUES (3, 'rust')`)).noErr(t)

	ordmap.Fork("test.db", "test2.db")

	assert(db.Exec(`INSERT INTO users (id, name) VALUES (4, 'java')`)).noErr(t)
	require.Equal(t, map[string]string{
		"0": "go",
		"1": "zig",
		"2": "whatever",
		"3": "rust",
		"4": "java",
	}, loadRows(t, db))

	// forked db should not see java
	db2 := assert(sql.Open("sqlite3", "file:/test2.db?vfs=ordmap")).noErr(t)
	defer db2.Close()

	require.Equal(t, map[string]string{
		"0": "go",
		"1": "zig",
		"2": "whatever",
		"3": "rust",
	}, loadRows(t, db2))
}

func loadRows(t *testing.T, db *sql.DB) map[string]string {
	rows, err := db.Query(`SELECT id, name FROM users`)
	require.NoError(t, err)
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var id, name string
		require.NoError(t, rows.Scan(&id, &name))
		result[id] = name
	}

	return result
}

type assertion[A any] struct {
	a   A
	err error
}

func assert[A any](a A, err error) *assertion[A] {
	return &assertion[A]{a: a, err: err}
}

func (a *assertion[A]) noErr(t *testing.T) A {
	require.NoError(t, a.err)
	return a.a
}
