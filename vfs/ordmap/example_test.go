package memdb_test

import (
	"database/sql"
	_ "embed"
	"log"
	"testing"

	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
	memdb "github.com/ncruces/go-sqlite3/vfs/ordmap"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/test.db
var testDB []byte

func TestExample(t *testing.T) {
	memdb.Create("test.db", testDB)

	db, err := sql.Open("sqlite3", "file:/test.db?vfs=memdb")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`INSERT INTO users (id, name) VALUES (3, 'rust')`)
	if err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query(`SELECT id, name FROM users`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	actual := [][]string{}
	for rows.Next() {
		var id, name string
		err = rows.Scan(&id, &name)
		if err != nil {
			log.Fatal(err)
		}
		actual = append(actual, []string{id, name})
	}

	expected := [][]string{
		{"0", "go"},
		{"1", "zig"},
		{"2", "whatever"},
		{"3", "rust"},
	}

	require.Equal(t, expected, actual)

}
