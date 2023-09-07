package postgres

import (
	"github.com/doug-martin/goqu/v9"
)

func DialectOptions() *goqu.SQLDialectOptions {
	do := goqu.DefaultDialectOptions()
	do.PlaceHolderFragment = []byte("$")
	do.LeftSliceFragment = []byte("ARRAY[")
	do.RightSliceFragment = []byte("]")
	do.EmptySliceFragment = []byte("'{}'") // This is special case for Postgres to omit explicit type of array
	do.IncludePlaceholderNum = true
	return do
}

func init() {
	goqu.RegisterDialect("postgres", DialectOptions())
}
