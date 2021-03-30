package mssql

import (
	"strconv"
	"strings"
)

// buffer used to strings buffer and argument of the query.
type buffer struct {
	strings.Builder
	valueCount int
	arguments  []interface{}
}

// WriteValue query placeholder and append value to argument.
func (b *buffer) WriteValue(value interface{}) {
	b.valueCount++
	b.WriteString("@p")
	b.WriteString(strconv.Itoa(b.valueCount))
	b.arguments = append(b.arguments, value)
}

// AddArguments appends multiple arguments without writing placeholder query..
func (b *buffer) AddArguments(args ...interface{}) {
	b.arguments = append(b.arguments, args...)
}

func (b buffer) Arguments() []interface{} {
	return b.arguments
}

// Reset buffer.
func (b *buffer) Reset() {
	b.Builder.Reset()
	b.valueCount = 0
	b.arguments = nil
}
