package goqu

import (
	"github.com/doug-martin/goqu/v9/exec"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/doug-martin/goqu/v9/internal/sb"
)

// TruncateDataset for creating and/or executing TRUNCATE SQL statements.
type TruncateDataset struct {
	dialect      SQLDialect
	clauses      exp.TruncateClauses
	isPrepared   prepared
	queryFactory exec.QueryFactory
	err          error
}

// used internally by database to create a database with a specific adapter.
func newTruncateDataset(d string, queryFactory exec.QueryFactory) *TruncateDataset {
	return &TruncateDataset{
		clauses:      exp.NewTruncateClauses(),
		dialect:      GetDialect(d),
		queryFactory: queryFactory,
	}
}

// Truncate creates TruncateDataset for a table.
func Truncate(table ...interface{}) *TruncateDataset {
	return newTruncateDataset("default", nil).Table(table...)
}

// WithDialect sets the adapter used to serialize values and create the SQL statement.
func (td *TruncateDataset) WithDialect(dl string) *TruncateDataset {
	ds := td.copy(td.GetClauses())
	ds.dialect = GetDialect(dl)
	return ds
}

// Prepared sets the parameter interpolation behavior.
//
// prepared: If true the dataset WILL NOT interpolate the parameters.
func (td *TruncateDataset) Prepared(prepared bool) *TruncateDataset {
	ret := td.copy(td.clauses)
	ret.isPrepared = preparedFromBool(prepared)
	return ret
}

// IsPrepared returns whether the TruncateDataset is prepared or not.
func (td *TruncateDataset) IsPrepared() bool {
	return td.isPrepared.Bool()
}

// Dialect returns the current adapter on the TruncateDataset.
func (td *TruncateDataset) Dialect() SQLDialect {
	return td.dialect
}

// SetDialect returns the current adapter on the TruncateDataset.
func (td *TruncateDataset) SetDialect(dialect SQLDialect) *TruncateDataset {
	cd := td.copy(td.GetClauses())
	cd.dialect = dialect
	return cd
}

// Expression returns TruncateDataset as exp.Expression.
func (td *TruncateDataset) Expression() exp.Expression {
	return td
}

// Clone clones the TruncateDataset.
func (td *TruncateDataset) Clone() exp.Expression {
	return td.copy(td.clauses)
}

// GetClauses returns the current clauses on the TruncateDataset.
func (td *TruncateDataset) GetClauses() exp.TruncateClauses {
	return td.clauses
}

// used internally to copy the dataset.
func (td *TruncateDataset) copy(clauses exp.TruncateClauses) *TruncateDataset {
	return &TruncateDataset{
		dialect:      td.dialect,
		clauses:      clauses,
		isPrepared:   td.isPrepared,
		queryFactory: td.queryFactory,
		err:          td.err,
	}
}

// Table adds a FROM clause. This return a new TruncateDataset with the original sources replaced.
// You can pass in the following.
//
// string: Will automatically be turned into an identifier
// IdentifierExpression
// LiteralExpression: (See Literal) Will use the literal SQL
func (td *TruncateDataset) Table(table ...interface{}) *TruncateDataset {
	return td.copy(td.clauses.SetTable(exp.NewColumnListExpression(table...)))
}

// Cascade adds a CASCADE clause.
func (td *TruncateDataset) Cascade() *TruncateDataset {
	opts := td.clauses.Options()
	opts.Cascade = true
	return td.copy(td.clauses.SetOptions(opts))
}

// NoCascade clears the CASCADE clause.
func (td *TruncateDataset) NoCascade() *TruncateDataset {
	opts := td.clauses.Options()
	opts.Cascade = false
	return td.copy(td.clauses.SetOptions(opts))
}

// Restrict adds a RESTRICT clause.
func (td *TruncateDataset) Restrict() *TruncateDataset {
	opts := td.clauses.Options()
	opts.Restrict = true
	return td.copy(td.clauses.SetOptions(opts))
}

// NoRestrict clears the RESTRICT clause.
func (td *TruncateDataset) NoRestrict() *TruncateDataset {
	opts := td.clauses.Options()
	opts.Restrict = false
	return td.copy(td.clauses.SetOptions(opts))
}

// Identity adds a IDENTITY clause (e.g. RESTART)
func (td *TruncateDataset) Identity(identity string) *TruncateDataset {
	opts := td.clauses.Options()
	opts.Identity = identity
	return td.copy(td.clauses.SetOptions(opts))
}

// Error returns any error that has been set or nil if no error has been set.
func (td *TruncateDataset) Error() error {
	return td.err
}

// SetError sets an error on the TruncateDataset if one has not already been set.
// This error will be returned by a future call to Error or as part of ToSQL.
// This can be used by end users to record errors while building up queries without having to track those separately.
func (td *TruncateDataset) SetError(err error) *TruncateDataset {
	if td.err == nil {
		td.err = err
	}

	return td
}

// ToSQL generates a TRUNCATE sql statement,
// if Prepared has been called with true then the parameters will not be interpolated.
//
// Errors:
//   - There is an error generating the SQL
func (td *TruncateDataset) ToSQL() (sql string, params []interface{}, err error) {
	return td.truncateSQLBuilder().ToSQL()
}

// MustToSQL does the same as ToSQL, but panics instead of returning an error.
func (td *TruncateDataset) MustToSQL() (sql string, params []interface{}) {
	var err error
	if sql, params, err = td.truncateSQLBuilder().ToSQL(); err != nil {
		panic(err)
	}
	return
}

// Executor generates the TRUNCATE sql, and returns an Exec struct with the sql set to the TRUNCATE statement.
//
// db.From("test").Truncate().Executor().Exec()
func (td *TruncateDataset) Executor() exec.QueryExecutor {
	return td.queryFactory.FromSQLBuilder(td.truncateSQLBuilder())
}

func (td *TruncateDataset) truncateSQLBuilder() sb.SQLBuilder {
	buf := sb.NewSQLBuilder(td.isPrepared.Bool())
	if td.err != nil {
		return buf.SetError(td.err)
	}
	td.dialect.ToTruncateSQL(buf, td.clauses)
	return buf
}
