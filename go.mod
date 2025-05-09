module github.com/ncruces/go-sqlite3

go 1.23.0

toolchain go1.24.0

require (
	github.com/ncruces/julianday v1.0.0
	github.com/ncruces/sort v0.1.5
	github.com/stretchr/testify v1.10.0
	github.com/tetratelabs/wazero v1.9.0
	golang.org/x/crypto v0.37.0
	golang.org/x/sys v0.32.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/edofic/go-ordmap/v2 v2.0.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

require (
	github.com/dchest/siphash v1.2.3 // ext/bloom
	github.com/google/uuid v1.6.0 // ext/uuid
	github.com/psanford/httpreadat v0.1.0 // example
	golang.org/x/sync v0.13.0 // test
	golang.org/x/text v0.24.0 // ext/unicode
	lukechampine.com/adiantum v1.1.1 // vfs/adiantum
)

retract (
	v0.23.2 // tagged from the wrong branch
	v0.4.0 // tagged from the wrong branch
)
