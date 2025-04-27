package ordmap_test

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
	_ "github.com/ncruces/go-sqlite3/vfs/memdb"
	"github.com/ncruces/go-sqlite3/vfs/ordmap"
	"github.com/stretchr/testify/require"
)

var (
	testVfsNames = []string{"ordmap", "memdb"}
	testRowNums  = []int{1, 10, 100, 1000}
)

func getDb(t require.TestingT, vfs string) *sql.DB {
	db, err := sql.Open("sqlite3", "file:test.db?vfs="+vfs)
	require.NoError(t, err)
	return db
}

func setupDb(t require.TestingT, db *sql.DB, num int) {
	_, err := db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, value INTEGER)")
	require.NoError(t, err)

	if num > 0 {
		tx, err := db.Begin()
		require.NoError(t, err)
		defer tx.Rollback()

		stmt, err := tx.Prepare("INSERT INTO test (name, value) VALUES (?, ?)")
		require.NoError(t, err)
		defer stmt.Close()

		for i := 0; i < num; i++ {
			_, err := stmt.Exec(fmt.Sprintf("Item %d", i), i)
			require.NoError(t, err)
		}

		err = tx.Commit()
		require.NoError(t, err)
	}
}

func BenchmarkInsert(b *testing.B) {
	for _, numRows := range testRowNums {
		for _, vfs := range testVfsNames {
			b.Run(fmt.Sprintf("%v/%v", vfs, numRows), func(b *testing.B) {
				db := getDb(b, vfs)
				defer db.Close()
				setupDb(b, db, 0) // Create table, but insert 0 initial rows

				// Prepare insert statement outside the loop for efficiency
				stmt, err := db.Prepare("INSERT INTO test (name, value) VALUES (?, ?)")
				require.NoError(b, err)
				defer stmt.Close() // Close the prepared statement

				b.ReportAllocs()
				for b.Loop() {
					tx, err := db.Begin()
					require.NoError(b, err)

					for j := 0; j < numRows; j++ {
						_, err := tx.Stmt(stmt).Exec(fmt.Sprintf("Bench Item %d", j), j)
						require.NoError(b, err)
					}

					err = tx.Commit()
					require.NoError(b, err)
				}
			})
		}
	}
}

func BenchmarkQuery(b *testing.B) {
	for _, numRows := range testRowNums {
		for _, vfs := range testVfsNames {
			b.Run(fmt.Sprintf("%v/%v", vfs, numRows), func(b *testing.B) {
				db := getDb(b, vfs)
				defer db.Close()
				setupDb(b, db, numRows) // Create table and insert numRows for querying

				stmt, err := db.Prepare("SELECT id, name, value FROM test")
				require.NoError(b, err)
				defer stmt.Close()

				b.ReportAllocs()
				for b.Loop() {
					rows, err := stmt.Query()
					require.NoError(b, err)
					defer rows.Close()

					// Iterate and scan results - necessary to measure the cost of reading data
					for rows.Next() {
						var id int
						var name string
						var value int
						err := rows.Scan(&id, &name, &value)
						require.NoError(b, err)
						_ = id
						_ = name
						_ = value
					}
					require.NoError(b, rows.Err()) // Check for errors that might have occurred during row iteration
					require.NoError(b, rows.Close())
				}
			})
		}
	}
}

func BenchmarkFork(b *testing.B) {
	db, err := sql.Open("sqlite3", "file:/base.db?vfs=ordmap")
	require.NoError(b, err)
	defer db.Close()
	setupDb(b, db, 1000)

	names := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		names[i] = fmt.Sprintf("forked_%d.db", i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for _, name := range names {
		ordmap.Fork("base.db", name)
	}
	b.StopTimer()

	for _, name := range names {
		ordmap.Delete(name)
	}
}
