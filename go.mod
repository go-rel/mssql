module github.com/go-rel/mssql

go 1.20

require (
	github.com/go-rel/rel v0.40.0
	github.com/go-rel/sql v0.15.0
	github.com/microsoft/go-mssqldb v1.6.0
	github.com/stretchr/testify v1.8.4
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/serenize/snaker v0.0.0-20201027110005-a7ad2135616e // indirect
	golang.org/x/crypto v0.12.0 // indirect
	golang.org/x/text v0.12.0 // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/go-rel/sql v0.14.0 => github.com/lafriks-fork/sql v0.15.1-0.20230814132010-79b68b85c382
