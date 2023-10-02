package goqu

import (
	"github.com/doug-martin/goqu/v9/exec"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/doug-martin/goqu/v9/internal/errors"
	"github.com/doug-martin/goqu/v9/internal/sb"
)

var ErrBadFromArgument = errors.New(
	"unsupported DeleteDataset#From argument, a string or identifier expression is required")

// DeleteDataset for creating and/or executing DELETE SQL statements.
type DeleteDataset struct {
	dialect      SQLDialect
	clauses      exp.DeleteClauses
	isPrepared   prepared
	queryFactory exec.QueryFactory
	err          error
}

// used internally by database to create a database with a specific adapter
func newDeleteDataset(d string, queryFactory exec.QueryFactory) *DeleteDataset {
	return &DeleteDataset{
		clauses:      exp.NewDeleteClauses(),
		dialect:      GetDialect(d),
		queryFactory: queryFactory,
		isPrepared:   preparedNoPreference,
		err:          nil,
	}
}

// Delete returns DeleteDataset for a table.
func Delete(table interface{}) *DeleteDataset {
	return newDeleteDataset("default", nil).From(table)
}

// Expression returns DeleteDataset as exp.Expression.
func (dd *DeleteDataset) Expression() exp.Expression {
	return dd
}

// Clone clones the DeleteDataset.
func (dd *DeleteDataset) Clone() exp.Expression {
	return dd.copy(dd.clauses)
}

// Prepared set the parameter interpolation behavior.
//
// prepared: If true the dataset WILL NOT interpolate the parameters.
func (dd *DeleteDataset) Prepared(prepared bool) *DeleteDataset {
	ret := dd.copy(dd.clauses)
	ret.isPrepared = preparedFromBool(prepared)
	return ret
}

// IsPrepared returns true if Prepared(true) has been called on this DeleteDataset.
func (dd *DeleteDataset) IsPrepared() bool {
	return dd.isPrepared.Bool()
}

// WithDialect sets the adapter used to serialize values and create the SQL statement.
func (dd *DeleteDataset) WithDialect(dl string) *DeleteDataset {
	ds := dd.copy(dd.GetClauses())
	ds.dialect = GetDialect(dl)
	return ds
}

// Dialect returns the current SQLDialect on the DeleteDataset.
func (dd *DeleteDataset) Dialect() SQLDialect {
	return dd.dialect
}

// SetDialect sets the SQLDialect for this DeleteDataset.
func (dd *DeleteDataset) SetDialect(dialect SQLDialect) *DeleteDataset {
	cd := dd.copy(dd.GetClauses())
	cd.dialect = dialect
	return cd
}

// GetClauses returns the current exp.DeleteClauses on the DeleteDataset.
func (dd *DeleteDataset) GetClauses() exp.DeleteClauses {
	return dd.clauses
}

// used internally to copy the DeleteDataset.
func (dd *DeleteDataset) copy(clauses exp.DeleteClauses) *DeleteDataset {
	return &DeleteDataset{
		dialect:      dd.dialect,
		clauses:      clauses,
		isPrepared:   dd.isPrepared,
		queryFactory: dd.queryFactory,
		err:          dd.err,
	}
}

// With creates a WITH clause for a common table expression (CTE).
//
// The name will be available to SELECT from in the associated query; and can optionally
// contain a list of column names "name(col1, col2, col3)".
//
// The name will refer to the results of the specified subquery.
func (dd *DeleteDataset) With(name string, subquery exp.Expression) *DeleteDataset {
	return dd.copy(dd.clauses.CommonTablesAppend(exp.NewCommonTableExpression(false, name, subquery)))
}

// WithRecursive creates a WITH RECURSIVE clause for a common table expression (CTE)
//
// The name will be available to SELECT from in the associated query; and must
// contain a list of column names "name(col1, col2, col3)" for a recursive clause.
//
// The name will refer to the results of the specified subquery. The subquery for
// a recursive query will always end with a UNION or UNION ALL with a clause that
// refers to the CTE by name.
func (dd *DeleteDataset) WithRecursive(name string, subquery exp.Expression) *DeleteDataset {
	return dd.copy(dd.clauses.CommonTablesAppend(exp.NewCommonTableExpression(true, name, subquery)))
}

// From adds a FROM clause. This return a new DeleteDataset with the original sources replaced.
// You can pass in the following.
//
// string: Will automatically be turned into an identifier
// Dataset: Will be added as a sub select. If the DeleteDataset is not aliased it will automatically be aliased
// LiteralExpression: (See Literal) Will use the literal SQL
func (dd *DeleteDataset) From(table interface{}) *DeleteDataset {
	switch t := table.(type) {
	case exp.IdentifierExpression:
		return dd.copy(dd.clauses.SetFrom(t))
	case string:
		return dd.copy(dd.clauses.SetFrom(exp.ParseIdentifier(t)))
	default:
		panic(ErrBadFromArgument)
	}
}

// Where adds a WHERE clause.
func (dd *DeleteDataset) Where(expressions ...exp.Expression) *DeleteDataset {
	return dd.copy(dd.clauses.WhereAppend(expressions...))
}

// ClearWhere removes the WHERE clause.
func (dd *DeleteDataset) ClearWhere() *DeleteDataset {
	return dd.copy(dd.clauses.ClearWhere())
}

// Order adds a ORDER clause. If the ORDER is currently set it replaces it.
func (dd *DeleteDataset) Order(order ...exp.OrderedExpression) *DeleteDataset {
	return dd.copy(dd.clauses.SetOrder(order...))
}

// OrderAppend adds a more columns to the current ORDER BY clause.
// If no order has been previously specified it is the same as calling Order.
func (dd *DeleteDataset) OrderAppend(order ...exp.OrderedExpression) *DeleteDataset {
	return dd.copy(dd.clauses.OrderAppend(order...))
}

// OrderPrepend adds a more columns to the beginning of the current ORDER BY clause.
// If no order has been previously specified it is the same as calling Order.
func (dd *DeleteDataset) OrderPrepend(order ...exp.OrderedExpression) *DeleteDataset {
	return dd.copy(dd.clauses.OrderPrepend(order...))
}

// ClearOrder removes the ORDER BY clause.
func (dd *DeleteDataset) ClearOrder() *DeleteDataset {
	return dd.copy(dd.clauses.ClearOrder())
}

// Limit adds a LIMIT clause. If the LIMIT is currently set it replaces it.
func (dd *DeleteDataset) Limit(limit uint) *DeleteDataset {
	if limit > 0 {
		return dd.copy(dd.clauses.SetLimit(limit))
	}
	return dd.copy(dd.clauses.ClearLimit())
}

// LimitAll adds a LIMIT ALL clause. If the LIMIT is currently set it replaces it.
func (dd *DeleteDataset) LimitAll() *DeleteDataset {
	return dd.copy(dd.clauses.SetLimit(L("ALL")))
}

// ClearLimit removes the LIMIT clause.
func (dd *DeleteDataset) ClearLimit() *DeleteDataset {
	return dd.copy(dd.clauses.ClearLimit())
}

// Returning adds a RETURNING clause to the DeleteDataset if the adapter supports it.
func (dd *DeleteDataset) Returning(returning ...interface{}) *DeleteDataset {
	return dd.copy(dd.clauses.SetReturning(exp.NewColumnListExpression(returning...)))
}

// Error returns any error that has been set or nil if no error has been set.
func (dd *DeleteDataset) Error() error {
	return dd.err
}

// SetError sets an error on the DeleteDataset if one has not already been set.
// This error will be returned by a future call to Error or as part of ToSQL.
// This can be used by end users to record errors while building up queries without having to track those separately.
func (dd *DeleteDataset) SetError(err error) *DeleteDataset {
	if dd.err == nil {
		dd.err = err
	}

	return dd
}

// ToSQL generates a DELETE sql statement,
// if Prepared has been called with true then the parameters will not be interpolated.
//
// Errors:
//   - There is an error generating the SQL
func (dd *DeleteDataset) ToSQL() (sql string, params []interface{}, err error) {
	return dd.deleteSQLBuilder().ToSQL()
}

// MustToSQL does the same as ToSQL, but panics instead of returning an error.
func (dd *DeleteDataset) MustToSQL() (sql string, params []interface{}) {
	var err error
	if sql, params, err = dd.deleteSQLBuilder().ToSQL(); err != nil {
		panic(err)
	}
	return
}

// AppendSQL appends this DeleteDataset's DELETE statement to the SQLBuilder.
// This is used internally when using deletes in CTEs.
func (dd *DeleteDataset) AppendSQL(b sb.SQLBuilder) {
	if dd.err != nil {
		b.SetError(dd.err)
		return
	}
	dd.dialect.ToDeleteSQL(b, dd.GetClauses())
}

// GetAs returns nothing
func (dd *DeleteDataset) GetAs() exp.IdentifierExpression {
	return nil
}

// ReturnsColumns returns whether the DeleteDataset has returning columns or not.
func (dd *DeleteDataset) ReturnsColumns() bool {
	return dd.clauses.HasReturning()
}

// Executor creates an QueryExecutor to execute the query.
//
// db.Delete("test").Exec()
//
// See Dataset#ToUpdateSQL for arguments
func (dd *DeleteDataset) Executor() exec.QueryExecutor {
	return dd.queryFactory.FromSQLBuilder(dd.deleteSQLBuilder())
}

func (dd *DeleteDataset) deleteSQLBuilder() sb.SQLBuilder {
	buf := sb.NewSQLBuilder(dd.isPrepared.Bool())
	if dd.err != nil {
		return buf.SetError(dd.err)
	}
	dd.dialect.ToDeleteSQL(buf, dd.clauses)
	return buf
}
