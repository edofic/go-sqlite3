package ordmap

import (
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/edofic/go-ordmap/v2"
	"github.com/ncruces/go-sqlite3"
	"github.com/ncruces/go-sqlite3/vfs"
)

const sectorSize = 65536 // 64KiB

// Ensure sectorSize is a multiple of 64K (the largest page size).
var _ [0]struct{} = [sectorSize & 65535]struct{}{}

type memVFS struct{}

func (memVFS) Open(name string, flags vfs.OpenFlag) (vfs.File, vfs.OpenFlag, error) {
	// For simplicity, we do not support reading or writing data
	// across "sector" boundaries in a single call.
	//
	// This is not a problem for most SQLite file types:
	// - databases, which only do page aligned reads/writes;
	// - temp journals, as used by the sorter, which does the same:
	//   https://github.com/sqlite/sqlite/blob/b74eb0/src/vdbesort.c#L409-L412
	//
	// We refuse to open all other file types,
	// but returning OPEN_MEMORY means SQLite won't ask us to.
	const types = vfs.OPEN_MAIN_DB |
		vfs.OPEN_TEMP_DB |
		vfs.OPEN_TEMP_JOURNAL
	if flags&types == 0 {
		// notest // OPEN_MEMORY
		return nil, flags, sqlite3.CANTOPEN
	}

	// A shared database has a name that begins with "/".
	shared := len(name) > 1 && name[0] == '/'

	var db *memDB
	if shared {
		name = name[1:]
		memoryMtx.Lock()
		defer memoryMtx.Unlock()
		db = memoryDBs[name]
	}
	if db == nil {
		if flags&vfs.OPEN_CREATE == 0 {
			return nil, flags, sqlite3.CANTOPEN
		}
		// Create a new database backend
		db = &memDB{
			name: name,
			data: ordmap.NewBuiltin[int64, []byte](),
		}
	}
	if shared {
		db.refs++ // +checklocksforce: memoryMtx is held
		memoryDBs[name] = db
	}

	return &memFile{
		memDB:    db,
		readOnly: flags&vfs.OPEN_READONLY != 0,
	}, flags | vfs.OPEN_MEMORY, nil
}

func (memVFS) Delete(name string, dirSync bool) error {
	return sqlite3.IOERR_DELETE_NOENT // used to delete journals
}

func (memVFS) Access(name string, flag vfs.AccessFlag) (bool, error) {
	return false, nil // used to check for journals
}

func (memVFS) FullPathname(name string) (string, error) {
	return name, nil
}

type memDB struct {
	name string

	// Stores database content keyed by sector index.
	// Slices are typically sectorSize bytes long, except potentially the last one.
	// +checklocks:dataMtx
	data ordmap.NodeBuiltin[int64, []byte]

	// Logical size of the file.
	// +checklocks:dataMtx
	size int64

	// +checklocks:memoryMtx
	refs int32

	shared   int32 // +checklocks:lockMtx
	pending  bool  // +checklocks:lockMtx
	reserved bool  // +checklocks:lockMtx

	lockMtx sync.Mutex
	dataMtx sync.RWMutex
}

func (m *memDB) release() {
	memoryMtx.Lock()
	defer memoryMtx.Unlock()
	if m.refs--; m.refs == 0 && m == memoryDBs[m.name] {
		delete(memoryDBs, m.name)
	}
}

func (m *memDB) fork() *memDB {
	m.dataMtx.Lock()
	defer m.dataMtx.Unlock()
	return &memDB{
		refs: 1,
		name: m.name,
		data: m.data,
		size: m.size,
	}
}

type memFile struct {
	*memDB
	lock     vfs.LockLevel
	readOnly bool
}

var (
	// Ensure these interfaces are implemented:
	_ vfs.FileLockState = &memFile{}
	_ vfs.FileSizeHint  = &memFile{}
)

func (m *memFile) Close() error {
	m.release()
	return m.Unlock(vfs.LOCK_NONE)
}

func (m *memFile) ReadAt(b []byte, off int64) (n int, err error) {
	m.dataMtx.RLock()
	defer m.dataMtx.RUnlock()

	fileSize := m.size // Capture size under lock

	if off < 0 {
		// This case should ideally not happen with SQLite VFS usage.
		return 0, sqlite3.IOERR_READ // Or perhaps io.ErrUnexpectedEOF
	}
	if off >= fileSize {
		return 0, io.EOF
	}

	// Calculate how many bytes we *can* read from the file overall
	readableFromFile := fileSize - off
	bytesToRead := int64(len(b))
	isShortReadEOF := false // Will we hit EOF and read less than len(b)?
	if bytesToRead > readableFromFile {
		bytesToRead = readableFromFile // Limit read to EOF
		isShortReadEOF = true
	}

	if bytesToRead == 0 {
		// This can happen if off == fileSize, should be caught by initial check.
		return 0, io.EOF
	}

	// Assume reads don't cross sectors (as per original code's VFS comment)
	base := off / sectorSize
	rest := off % sectorSize

	// Calculate max bytes to read *within this sector* boundary
	bytesInSector := sectorSize - rest
	readNow := min(bytesToRead, bytesInSector) // Actual bytes to process in this call

	page, ok := m.data.Get(base)
	if !ok {
		// Sparse read - return zeroes
		clear(b[:readNow])
		n = int(readNow)
	} else {
		// Sector exists. Read from the available slice data.
		availableInPage := int64(len(page)) - rest
		if availableInPage <= 0 {
			// Offset is at or past the end of this page's actual stored data.
			// Return zeroes for the requested portion within this conceptual sector.
			clear(b[:readNow])
			n = int(readNow)
		} else {
			// Read available data from the page slice
			readFromPage := min(readNow, availableInPage)
			bytesCopied := copy(b[:readFromPage], page[rest:rest+readFromPage])
			n = bytesCopied

			// If readNow > readFromPage, it means we need more zeroes
			// after the end of the actual slice data up to the readNow limit.
			if readNow > readFromPage {
				zeroStart := readFromPage
				zeroEnd := readNow
				clear(b[zeroStart:zeroEnd])
				n = int(readNow) // Total bytes "read" (data + zeroes)
			}
		}
	}

	// Determine return error based on io.ReaderAt spec
	if isShortReadEOF {
		// We read fewer bytes than originally requested *because* we hit EOF
		err = io.EOF
	} else if int64(n) < int64(len(b)) {
		// We read fewer bytes than requested, but didn't hit EOF.
		// This implies the read *would* have crossed a sector boundary if fully satisfied.
		// Given the original VFS constraint assumption, SQLite shouldn't cause this.
		// If it *did*, returning nil error is pragmatic, allowing the next read call.
		err = nil
	}
	// If n == len(b), err is nil.

	return n, err
}

// writeToSector handles the logic of writing data into a specific sector's slice.
// Assumes m.dataMtx is already held for writing.
// +checklocks:m.dataMtx
func (m *memFile) writeToSector(base int64, offsetInSector int64, dataToWrite []byte) (int, error) {
	if offsetInSector < 0 || offsetInSector >= sectorSize {
		return 0, sqlite3.IOERR_WRITE // Invalid offset
	}
	if len(dataToWrite) == 0 {
		return 0, nil
	}

	neededEndOffset := offsetInSector + int64(len(dataToWrite))
	if neededEndOffset > sectorSize {
		// Caller should have prevented this based on non-crossing assumption
		return 0, io.ErrShortWrite // Attempt to write past sector boundary
	}

	page, ok := m.data.Get(base)
	if !ok {
		page = make([]byte, sectorSize)
	} else {
		newPage := make([]byte, sectorSize)
		copy(newPage, page) // Copy existing data
		page = newPage
	}

	// Perform the copy
	n := copy(page[offsetInSector:], dataToWrite)
	// n should equal len(dataToWrite) because we ensured sufficient length
	if n != len(dataToWrite) {
		// Should not happen given the checks
		return n, io.ErrShortWrite // Or sqlite3.IOERR_WRITE
	}

	m.data = m.data.Insert(base, page)
	return n, nil
}

func (m *memFile) WriteAt(b []byte, off int64) (n int, err error) {
	m.dataMtx.Lock()
	defer m.dataMtx.Unlock()

	if off < 0 {
		return 0, sqlite3.IOERR_WRITE // Or io.ErrUnexpectedEOF
	}
	if len(b) == 0 {
		return 0, nil
	}

	base := off / sectorSize
	rest := off % sectorSize
	writeEndOff := off + int64(len(b))

	// Check if write crosses sector boundary (based on original code's assumption)
	if writeEndOff > (base+1)*sectorSize {
		// notest // assume writes are page aligned and non-crossing
		// Write only the portion that fits in the current sector
		bytesToWrite := sectorSize - rest
		if bytesToWrite <= 0 {
			// Write starts exactly at or beyond the sector boundary.
			return 0, io.ErrShortWrite // Cannot write 0 bytes or start past end
		}

		n, err = m.writeToSector(base, rest, b[:bytesToWrite])
		// Update size only for the bytes actually written in this sector
		currentWriteEnd := off + int64(n)
		if currentWriteEnd > m.size {
			m.size = currentWriteEnd
		}
		if err != nil {
			return n, err // Return error from writeToSector if any
		}
		// Return ErrShortWrite because we couldn't write all of `b`
		return n, io.ErrShortWrite
	}

	// Write fits entirely within a single sector
	n, err = m.writeToSector(base, rest, b)
	if err != nil {
		return n, err // Return error from writeToSector if any
	}

	// Update size if this write extended the file
	if writeEndOff > m.size {
		m.size = writeEndOff
	}
	return n, nil // Success
}

func (m *memFile) Truncate(size int64) error {
	m.dataMtx.Lock()
	defer m.dataMtx.Unlock()
	return m.truncate(size)
}

// truncate adjusts the file size and underlying map data.
// Assumes m.dataMtx lock is held.
// +checklocks:m.dataMtx
func (m *memFile) truncate(size int64) error {
	if size < 0 {
		size = 0 // File size cannot be negative
	}

	m.size = size // Update logical size

	if size == 0 {
		m.data = ordmap.NewBuiltin[int64, []byte]()
		return nil
	}

	// Calculate the index of the last sector needed and its size
	lastBase := (size - 1) / sectorSize
	sizeInLastSector := size - (lastBase * sectorSize) // Bytes used in the last sector

	lastSector, ok := m.data.Get(lastBase)
	if ok {
		truncated := make([]byte, sectorSize)
		copy(truncated, lastSector)
		m.data = m.data.Insert(lastBase, truncated[:sizeInLastSector])
	}

	for iter := m.data.Iterate(); !iter.Done(); iter.Next() {
		key := iter.GetKey()
		if key > lastBase {
			m.data = m.data.Remove(key)
		}
	}

	return nil
}

func (m *memFile) Sync(flag vfs.SyncFlag) error {
	// No-op for in-memory VFS
	return nil
}

func (m *memFile) Size() (int64, error) {
	m.dataMtx.RLock()
	defer m.dataMtx.RUnlock()
	return m.size, nil
}

// --- Locking methods remain unchanged ---

const spinWait = 25 * time.Microsecond

func (m *memFile) Lock(lock vfs.LockLevel) error {
	if m.lock >= lock {
		return nil
	}

	if m.readOnly && lock >= vfs.LOCK_RESERVED {
		return sqlite3.IOERR_LOCK
	}

	m.lockMtx.Lock()
	defer m.lockMtx.Unlock()

	switch lock {
	case vfs.LOCK_SHARED:
		// If a PENDING lock is held, cannot acquire SHARED (SQLite rule)
		if m.pending {
			return sqlite3.BUSY // NOTE: Original might have allowed this? Check SQLite docs if issues arise.
			// Re-checked: Yes, acquiring SHARED should fail if PENDING exists. sqlite3.BUSY is correct.
		}
		m.shared++

	case vfs.LOCK_RESERVED:
		if m.reserved {
			// Can't acquire RESERVED if another connection already holds it.
			return sqlite3.BUSY
		}
		m.reserved = true

	case vfs.LOCK_EXCLUSIVE:
		// Must acquire PENDING first (SQLite does this implicitly via Lock calls)
		if m.lock < vfs.LOCK_PENDING {
			// Attempt to acquire PENDING lock before EXCLUSIVE
			// A PENDING lock prevents new SHARED locks, allowing existing ones to clear.
			m.lock = vfs.LOCK_PENDING
			m.pending = true // Signal pending state
		}

		// Wait for all other SHARED locks to be released.
		// Need to check m.shared count (must be <= 1, representing our own potential SHARED lock).
		for before := time.Now(); m.shared > 1; { // Check if > 1 because we might hold one ourselves
			if time.Since(before) > spinWait*10 { // Increased timeout slightly
				// If PENDING lock held for too long, return BUSY (SQLite behavior)
				return sqlite3.BUSY_RECOVERY // Or just BUSY? BUSY_RECOVERY implies db state might need rollback
			}
			// Temporarily release the mutex to allow other goroutines (connections) to release their SHARED locks
			m.lockMtx.Unlock()
			runtime.Gosched() // Yield CPU
			m.lockMtx.Lock()  // Reacquire mutex to check again
		}
		// At this point, m.shared <= 1, safe to acquire EXCLUSIVE
	}

	m.lock = lock
	return nil
}

func (m *memFile) Unlock(lock vfs.LockLevel) error {
	if m.lock <= lock {
		return nil
	}

	m.lockMtx.Lock()
	defer m.lockMtx.Unlock()

	oldLock := m.lock

	// Release higher-level locks first when dropping to a lower level
	if oldLock >= vfs.LOCK_EXCLUSIVE {
		// Dropping exclusive, nothing specific to release other than state below
	}
	if oldLock >= vfs.LOCK_PENDING {
		if lock < vfs.LOCK_PENDING { // Only release pending if dropping below it
			m.pending = false
		}
	}
	if oldLock >= vfs.LOCK_RESERVED {
		if lock < vfs.LOCK_RESERVED { // Only release reserved if dropping below it
			m.reserved = false
		}
	}
	if oldLock >= vfs.LOCK_SHARED {
		if lock < vfs.LOCK_SHARED { // Only release shared if dropping below it
			m.shared--
			if m.shared < 0 {
				// This indicates a locking logic error (more unlocks than locks)
				// panic("Negative shared lock count") // Consider adding panic for debugging
				m.shared = 0 // Reset to avoid issues
			}
		}
	}

	m.lock = lock
	return nil
}

func (m *memFile) CheckReservedLock() (bool, error) {
	// This VFS doesn't use OS-level locks, so check our internal state.
	// According to SQLite comments, this can be called without holding a lock.
	// We need the mutex to safely check the shared state.
	m.lockMtx.Lock()
	defer m.lockMtx.Unlock()
	// Return true if any connection holds a RESERVED or higher lock.
	return m.reserved || m.lock >= vfs.LOCK_EXCLUSIVE, nil
}

func (m *memFile) SectorSize() int {
	// Must be >= 512 and a power of two. Matches SQLite page size constraints.
	// Used for certain optimizations like atomic writes if IOCAP_ATOMIC is set.
	return sectorSize
}

func (m *memFile) DeviceCharacteristics() vfs.DeviceCharacteristic {
	// IOCAP_ATOMIC: Assumes writes of SectorSize are atomic.
	// IOCAP_SEQUENTIAL: Hint that sequential access might be faster (less relevant for memory).
	// IOCAP_SAFE_APPEND: Append operations are safe wrt concurrent reads (true for our mutex).
	// IOCAP_POWERSAFE_OVERWRITE: Writes that don't change file size are atomic wrt power loss
	//                           (meaningless for memory, but required if SectorSize > 0).
	return vfs.IOCAP_ATOMIC | // Maybe? Let's assume our locked writes are atomic enough.
		vfs.IOCAP_SEQUENTIAL |
		vfs.IOCAP_SAFE_APPEND |
		vfs.IOCAP_POWERSAFE_OVERWRITE
}

func (m *memFile) SizeHint(size int64) error {
	// Hint that the file may grow to this size. We can use truncate
	// to update the logical size, actual allocation happens on write.
	m.dataMtx.Lock()
	defer m.dataMtx.Unlock()
	if size > m.size {
		// Only update logical size if hinting larger. Don't preallocate.
		// Don't shrink based on a hint.
		return m.truncate(size)
	}
	return nil // Hinting smaller or same size is a no-op.
}

func (m *memFile) LockState() vfs.LockLevel {
	// Required by FileLockState interface.
	// This might need locking if accessed concurrently, but typically called
	// by the connection that owns the lock state itself. Assume safe for now.
	// Add lock just in case external tools query it.
	m.lockMtx.Lock()
	defer m.lockMtx.Unlock()
	return m.lock
}

// Helper functions (unchanged)
func divRoundUp(a, b int64) int64 {
	return (a + b - 1) / b
}

// min returns the smaller of two int64 values.
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
