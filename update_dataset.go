package goqu

import (
	"github.com/doug-martin/goqu/v9/exec"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/doug-martin/goqu/v9/internal/errors"
	"github.com/doug-martin/goqu/v9/internal/sb"
)

// UpdateDataset for creating and/or executing UPDATE SQL statements.
type UpdateDataset struct {
	dialect      SQLDialect
	clauses      exp.UpdateClauses
	isPrepared   prepared
	queryFactory exec.QueryFactory
	err          error
}

var ErrUnsupportedUpdateTableType = errors.New("unsupported table type, a string or identifier expression is required")

// used internally by database to create a database with a specific adapter.
func newUpdateDataset(d string, queryFactory exec.QueryFactory) *UpdateDataset {
	return &UpdateDataset{
		clauses:      exp.NewUpdateClauses(),
		dialect:      GetDialect(d),
		queryFactory: queryFactory,
	}
}

// Update creates UpdateDataset for a table.
func Update(table interface{}) *UpdateDataset {
	return newUpdateDataset("default", nil).Table(table)
}

// Prepared sets the parameter interpolation behavior.
//
// prepared: If true the dataset WILL NOT interpolate the parameters.
func (ud *UpdateDataset) Prepared(prepared bool) *UpdateDataset {
	ret := ud.copy(ud.clauses)
	ret.isPrepared = preparedFromBool(prepared)
	return ret
}

// IsPrepared returns whether the UpdateDataset is prepared or not.
func (ud *UpdateDataset) IsPrepared() bool {
	return ud.isPrepared.Bool()
}

// WithDialect sets the adapter used to serialize values and create the SQL statement
func (ud *UpdateDataset) WithDialect(dl string) *UpdateDataset {
	ds := ud.copy(ud.GetClauses())
	ds.dialect = GetDialect(dl)
	return ds
}

// Dialect returns the current adapter on the UpdateDataset.
func (ud *UpdateDataset) Dialect() SQLDialect {
	return ud.dialect
}

// SetDialect returns the current adapter on the UpdateDataset.
func (ud *UpdateDataset) SetDialect(dialect SQLDialect) *UpdateDataset {
	cd := ud.copy(ud.GetClauses())
	cd.dialect = dialect
	return cd
}

// Expression returns UpdateDataset as exp.Expression.
func (ud *UpdateDataset) Expression() exp.Expression {
	return ud
}

// Clone clones the UpdateDataset.
func (ud *UpdateDataset) Clone() exp.Expression {
	return ud.copy(ud.clauses)
}

// GetClauses returns the current clauses on the UpdateDataset.
func (ud *UpdateDataset) GetClauses() exp.UpdateClauses {
	return ud.clauses
}

// used internally to copy the dataset.
func (ud *UpdateDataset) copy(clauses exp.UpdateClauses) *UpdateDataset {
	return &UpdateDataset{
		dialect:      ud.dialect,
		clauses:      clauses,
		isPrepared:   ud.isPrepared,
		queryFactory: ud.queryFactory,
		err:          ud.err,
	}
}

// With creates a WITH clause for a common table expression (CTE).
//
// The name will be available to use in the UPDATE from in the associated query; and can optionally
// contain a list of column names "name(col1, col2, col3)".
//
// The name will refer to the results of the specified subquery.
func (ud *UpdateDataset) With(name string, subquery exp.Expression) *UpdateDataset {
	return ud.copy(ud.clauses.CommonTablesAppend(exp.NewCommonTableExpression(false, name, subquery)))
}

// WithRecursive creates a WITH RECURSIVE clause for a common table expression (CTE)
//
// The name will be available to use in the UPDATE from in the associated query; and must
// contain a list of column names "name(col1, col2, col3)" for a recursive clause.
//
// The name will refer to the results of the specified subquery. The subquery for
// a recursive query will always end with a UNION or UNION ALL with a clause that
// refers to the CTE by name.
func (ud *UpdateDataset) WithRecursive(name string, subquery exp.Expression) *UpdateDataset {
	return ud.copy(ud.clauses.CommonTablesAppend(exp.NewCommonTableExpression(true, name, subquery)))
}

// Table sets the table to update.
func (ud *UpdateDataset) Table(table interface{}) *UpdateDataset {
	switch t := table.(type) {
	case exp.Expression:
		return ud.copy(ud.clauses.SetTable(t))
	case string:
		return ud.copy(ud.clauses.SetTable(exp.ParseIdentifier(t)))
	default:
		panic(ErrUnsupportedUpdateTableType)
	}
}

// Set sets the values to use in the SET clause.
func (ud *UpdateDataset) Set(values interface{}) *UpdateDataset {
	return ud.copy(ud.clauses.SetSetValues(values))
}

// From allows specifying other tables to reference in your update (If your dialect supports it).
func (ud *UpdateDataset) From(tables ...interface{}) *UpdateDataset {
	return ud.copy(ud.clauses.SetFrom(exp.NewColumnListExpression(tables...)))
}

// Where adds a WHERE clause.
func (ud *UpdateDataset) Where(expressions ...exp.Expression) *UpdateDataset {
	return ud.copy(ud.clauses.WhereAppend(expressions...))
}

// ClearWhere removes the WHERE clause.
func (ud *UpdateDataset) ClearWhere() *UpdateDataset {
	return ud.copy(ud.clauses.ClearWhere())
}

// Order adds a ORDER clause. If the ORDER is currently set it replaces it.
func (ud *UpdateDataset) Order(order ...exp.OrderedExpression) *UpdateDataset {
	return ud.copy(ud.clauses.SetOrder(order...))
}

// OrderAppend adds a more columns to the current ORDER BY clause.
// If no order has been previously specified it is the same as calling Order.
func (ud *UpdateDataset) OrderAppend(order ...exp.OrderedExpression) *UpdateDataset {
	return ud.copy(ud.clauses.OrderAppend(order...))
}

// OrderPrepend adds a more columns to the beginning of the current ORDER BY clause.
// If no order has been previously specified it is the same as calling Order.
func (ud *UpdateDataset) OrderPrepend(order ...exp.OrderedExpression) *UpdateDataset {
	return ud.copy(ud.clauses.OrderPrepend(order...))
}

// ClearOrder removes the ORDER BY clause.
func (ud *UpdateDataset) ClearOrder() *UpdateDataset {
	return ud.copy(ud.clauses.ClearOrder())
}

// Limit adds a LIMIT clause. If the LIMIT is currently set it replaces it.
func (ud *UpdateDataset) Limit(limit uint) *UpdateDataset {
	if limit > 0 {
		return ud.copy(ud.clauses.SetLimit(limit))
	}
	return ud.copy(ud.clauses.ClearLimit())
}

// LimitAll adds a LIMIT ALL clause. If the LIMIT is currently set it replaces it.
func (ud *UpdateDataset) LimitAll() *UpdateDataset {
	return ud.copy(ud.clauses.SetLimit(L("ALL")))
}

// ClearLimit removes the LIMIT clause.
func (ud *UpdateDataset) ClearLimit() *UpdateDataset {
	return ud.copy(ud.clauses.ClearLimit())
}

// Returning adds a RETURNING clause to the dataset if the adapter supports it.
func (ud *UpdateDataset) Returning(returning ...interface{}) *UpdateDataset {
	return ud.copy(ud.clauses.SetReturning(exp.NewColumnListExpression(returning...)))
}

// Error returns any error that has been set or nil if no error has been set.
func (ud *UpdateDataset) Error() error {
	return ud.err
}

// SetError sets an error on the UpdateDataset if one has not already been set.
// This error will be returned by a future call to Error or as part of ToSQL.
// This can be used by end users to record errors while building up queries without having to track those separately.
func (ud *UpdateDataset) SetError(err error) *UpdateDataset {
	if ud.err == nil {
		ud.err = err
	}

	return ud
}

// ToSQL generates an UPDATE sql statement,
// if Prepared has been called with true then the parameters will not be interpolated.
//
// Errors:
//   - There is an error generating the SQL
func (ud *UpdateDataset) ToSQL() (sql string, params []interface{}, err error) {
	return ud.updateSQLBuilder().ToSQL()
}

// MustToSQL does the same as ToSQL, but panics instead of returning an error.
func (ud *UpdateDataset) MustToSQL() (sql string, params []interface{}) {
	var err error
	if sql, params, err = ud.updateSQLBuilder().ToSQL(); err != nil {
		panic(err)
	}
	return
}

// AppendSQL appends this UpdateDataset's UPDATE statement to the SQLBuilder.
// This is used internally when using updates in CTEs.
func (ud *UpdateDataset) AppendSQL(b sb.SQLBuilder) {
	if ud.err != nil {
		b.SetError(ud.err)
		return
	}
	ud.dialect.ToUpdateSQL(b, ud.GetClauses())
}

// GetAs returns the alias value as an identifier expression.
func (ud *UpdateDataset) GetAs() exp.IdentifierExpression {
	return nil
}

// ReturnsColumns returns whether the SelectDataset has returning columns or not.
func (ud *UpdateDataset) ReturnsColumns() bool {
	return ud.clauses.HasReturning()
}

// Executor generates the UPDATE sql, and returns an exec.QueryExecutor with the sql set to the UPDATE statement.
//
// db.Update("test").Set(Record{"name":"Bob", update: time.Now()}).Executor()
func (ud *UpdateDataset) Executor() exec.QueryExecutor {
	return ud.queryFactory.FromSQLBuilder(ud.updateSQLBuilder())
}

func (ud *UpdateDataset) updateSQLBuilder() sb.SQLBuilder {
	buf := sb.NewSQLBuilder(ud.isPrepared.Bool())
	if ud.err != nil {
		return buf.SetError(ud.err)
	}
	ud.dialect.ToUpdateSQL(buf, ud.clauses)
	return buf
}
