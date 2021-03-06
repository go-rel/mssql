package mssql

import (
	"strings"
	"sync"
)

// UnescapeCharacter disable field escaping when it starts with this character.
var UnescapeCharacter byte = '^'

var fieldCache sync.Map

// FieldSQL builder.
type FieldSQL struct {
	prefix string
	suffix string
}

type fieldCacheKey struct {
	field    string
	fieldSQL FieldSQL
}

// Build return escaped field.
func (fs FieldSQL) Build(field string) string {
	if fs.prefix == "" && fs.suffix == "" || field == "*" {
		return field
	}

	key := fieldCacheKey{field: field, fieldSQL: fs}
	escapedField, ok := fieldCache.Load(key)
	if ok {
		return escapedField.(string)
	}

	if len(field) > 0 && field[0] == UnescapeCharacter {
		escapedField = field[1:]
	} else if i := strings.Index(strings.ToLower(field), " as "); i > -1 {
		escapedField = fs.Build(field[:i]) + " AS " + fs.Build(field[i+4:])
	} else if start, end := strings.IndexRune(field, '('), strings.IndexRune(field, ')'); start >= 0 && end >= 0 && end > start {
		escapedField = field[:start+1] + fs.Build(field[start+1:end]) + field[end:]
	} else if strings.HasSuffix(field, "*") {
		escapedField = fs.prefix + strings.Replace(field, ".", fs.suffix+".", 1)
	} else {
		escapedField = fs.prefix +
			strings.Replace(field, ".", fs.suffix+"."+fs.prefix, 1) +
			fs.suffix
	}

	fieldCache.Store(key, escapedField)
	return escapedField.(string)
}

// NewFieldSQL builder.
func NewFieldSQL(prefix string, suffix string) FieldSQL {
	return FieldSQL{prefix: prefix, suffix: suffix}
}
