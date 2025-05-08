// Package ordmap implements the "memdb" SQLite VFS.
//
// The "ordmap" [vfs.VFS] allows the same in-memory database to be shared
// among multiple database connections in the same process,
// as long as the database name begins with "/".
//
// Importing package ordmap registers the VFS:
//
//	import _ "github.com/ncruces/go-sqlite3/vfs/ordmap"
package ordmap

import (
	"fmt"
	"net/url"
	"sync"
	"testing"

	"github.com/edofic/go-ordmap/v2"
	"github.com/ncruces/go-sqlite3/vfs"
)

func init() {
	vfs.Register("ordmapmvcc", memVFS{})
}

var (
	memoryMtx sync.Mutex
	// +checklocks:memoryMtx
	memoryDBs = map[string]*memDB{}
)

// Create creates a shared memory database,
// using data as its initial contents.
// The new database takes ownership of data,
// and the caller should not use data after this call.
func Create(name string, data []byte) {
	memoryMtx.Lock()
	defer memoryMtx.Unlock()

	db := &memDB{
		refs: 1,
		name: name,
		data: ordmap.NewBuiltin[int64, []byte](),
		size: int64(len(data)),
	}

	// Convert data from WAL/2 to rollback journal.
	if len(data) >= 20 && (false ||
		data[18] == 2 && data[19] == 2 ||
		data[18] == 3 && data[19] == 3) {
		data[18] = 1
		data[19] = 1
	}

	sectors := divRoundUp(db.size, sectorSize)
	for i := int64(0); i < sectors; i++ {
		sector := make([]byte, sectorSize)
		copy(sector, data[i*sectorSize:])
		db.data = db.data.Insert(i, sector)
	}

	memoryDBs[name] = db
}

func Fork(name, newName string) {
	memoryMtx.Lock()
	defer memoryMtx.Unlock()
	memoryDBs[newName] = memoryDBs[name].fork()
}

// Delete deletes a shared memory database.
func Delete(name string) {
	memoryMtx.Lock()
	defer memoryMtx.Unlock()
	delete(memoryDBs, name)
}

// TestDB creates an empty shared memory database for the test to use.
// The database is automatically deleted when the test and all its subtests complete.
// Each subsequent call to TestDB returns a unique database.
func TestDB(tb testing.TB, params ...url.Values) string {
	tb.Helper()

	name := fmt.Sprintf("%s_%p", tb.Name(), tb)
	tb.Cleanup(func() { Delete(name) })
	Create(name, nil)

	p := url.Values{"vfs": {"memdb"}}
	for _, v := range params {
		for k, v := range v {
			for _, v := range v {
				p.Add(k, v)
			}
		}
	}

	return (&url.URL{
		Scheme:   "file",
		OmitHost: true,
		Path:     "/" + name,
		RawQuery: p.Encode(),
	}).String()
}
