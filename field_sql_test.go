package mssql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFieldSQL_Build(t *testing.T) {
	fieldSQL := NewFieldSQL("[", "]")

	tests := []struct {
		field  string
		result string
	}{
		{
			field:  "count(*) as count",
			result: "count(*) AS [count]",
		},
		{
			field:  "user.address as home_address",
			result: "[user].[address] AS [home_address]",
		},
		{
			field:  "^FIELD([gender], \"male\") AS order",
			result: "FIELD([gender], \"male\") AS order",
		},
		{
			field:  "*",
			result: "*",
		},
		{
			field:  "user.*",
			result: "[user].*",
		},
	}
	for _, test := range tests {
		t.Run(test.result, func(t *testing.T) {
			var (
				result = fieldSQL.Build(test.field)
			)

			assert.Equal(t, test.result, result)
		})
	}
}
