package postgres

import (
	"github.com/doug-martin/goqu/v9"
)

func DialectOptions() *goqu.SQLDialectOptions {
	do := goqu.DefaultDialectOptions()
	do.PlaceHolderFragment = []byte("$")
	do.LeftSliceFragment = []byte("'{")
	do.RightSliceFragment = []byte("}'")
	do.StringSliceQuote = '"'
	do.SinglePlaceholderForSlice = true
	do.IncludePlaceholderNum = true
	return do
}

func init() {
	goqu.RegisterDialect("postgres", DialectOptions())
}
