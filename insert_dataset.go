package goqu

import (
	"fmt"

	"github.com/doug-martin/goqu/v9/exec"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/doug-martin/goqu/v9/internal/errors"
	"github.com/doug-martin/goqu/v9/internal/sb"
)

// InsertDataset for creating and/or executing INSERT SQL statements.
type InsertDataset struct {
	dialect      SQLDialect
	clauses      exp.InsertClauses
	isPrepared   prepared
	queryFactory exec.QueryFactory
	err          error
}

var ErrUnsupportedIntoType = errors.New("unsupported table type, a string or identifier expression is required")

// used internally by database to create a database with a specific adapter.
func newInsertDataset(d string, queryFactory exec.QueryFactory) *InsertDataset {
	return &InsertDataset{
		clauses:      exp.NewInsertClauses(),
		dialect:      GetDialect(d),
		queryFactory: queryFactory,
	}
}

// Insert creates a new InsertDataset for the provided table. Using this method will only allow you
// to create SQL user Database#From to create an InsertDataset with query capabilities.
func Insert(table interface{}) *InsertDataset {
	return newInsertDataset("default", nil).Into(table)
}

// Prepared sets the parameter interpolation behavior.
//
// prepared: If true the InsertDataset WILL NOT interpolate the parameters.
func (id *InsertDataset) Prepared(prepared bool) *InsertDataset {
	ret := id.copy(id.clauses)
	ret.isPrepared = preparedFromBool(prepared)
	return ret
}

// IsPrepared returns whether the InsertDataset is prepared or not.
func (id *InsertDataset) IsPrepared() bool {
	return id.isPrepared.Bool()
}

// WithDialect sets the adapter used to serialize values and create the SQL statement.
func (id *InsertDataset) WithDialect(dl string) *InsertDataset {
	ds := id.copy(id.GetClauses())
	ds.dialect = GetDialect(dl)
	return ds
}

// Dialect returns the current adapter on the dataset.
func (id *InsertDataset) Dialect() SQLDialect {
	return id.dialect
}

// SetDialect sets the current adapter on the dataset.
func (id *InsertDataset) SetDialect(dialect SQLDialect) *InsertDataset {
	cd := id.copy(id.GetClauses())
	cd.dialect = dialect
	return cd
}

// Expression returns InsertDataset as exp.Expression.
func (id *InsertDataset) Expression() exp.Expression {
	return id
}

// Clone clones the InsertDataset.
func (id *InsertDataset) Clone() exp.Expression {
	return id.copy(id.clauses)
}

// GetClauses returns the current clauses on the InsertDataset.
func (id *InsertDataset) GetClauses() exp.InsertClauses {
	return id.clauses
}

// used internally to copy the InsertDataset.
func (id *InsertDataset) copy(clauses exp.InsertClauses) *InsertDataset {
	return &InsertDataset{
		dialect:      id.dialect,
		clauses:      clauses,
		isPrepared:   id.isPrepared,
		queryFactory: id.queryFactory,
		err:          id.err,
	}
}

// With creates a WITH clause for a common table expression (CTE).
//
// The name will be available to SELECT from in the associated query; and can optionally
// contain a list of column names "name(col1, col2, col3)".
//
// The name will refer to the results of the specified subquery.
func (id *InsertDataset) With(name string, subquery exp.Expression) *InsertDataset {
	return id.copy(id.clauses.CommonTablesAppend(exp.NewCommonTableExpression(false, name, subquery)))
}

// WithRecursive creates a WITH RECURSIVE clause for a common table expression (CTE)
//
// The name will be available to SELECT from in the associated query; and must
// contain a list of column names "name(col1, col2, col3)" for a recursive clause.
//
// The name will refer to the results of the specified subquery. The subquery for
// a recursive query will always end with a UNION or UNION ALL with a clause that
// refers to the CTE by name.
func (id *InsertDataset) WithRecursive(name string, subquery exp.Expression) *InsertDataset {
	return id.copy(id.clauses.CommonTablesAppend(exp.NewCommonTableExpression(true, name, subquery)))
}

// Into sets the table to insert INTO. This return a new InsertDataset with the original table replaced.
// You can pass in the following.
//
// string: Will automatically be turned into an identifier
// expression: any valid exp.Expression (exp.IdentifierExpression, exp.AliasedExpression, Literal, etc.)
func (id *InsertDataset) Into(into interface{}) *InsertDataset {
	switch t := into.(type) {
	case exp.Expression:
		return id.copy(id.clauses.SetInto(t))
	case string:
		return id.copy(id.clauses.SetInto(exp.ParseIdentifier(t)))
	default:
		panic(ErrUnsupportedIntoType)
	}
}

// Cols sets the Columns to insert into.
func (id *InsertDataset) Cols(cols ...interface{}) *InsertDataset {
	return id.copy(id.clauses.SetCols(exp.NewColumnListExpression(cols...)))
}

// ClearCols clears the Columns to insert into.
func (id *InsertDataset) ClearCols() *InsertDataset {
	return id.copy(id.clauses.SetCols(nil))
}

// ColsAppend adds columns to the current list of columns clause.
func (id *InsertDataset) ColsAppend(cols ...interface{}) *InsertDataset {
	return id.copy(id.clauses.ColsAppend(exp.NewColumnListExpression(cols...)))
}

// FromQuery adds a subquery to the insert.
func (id *InsertDataset) FromQuery(from exp.AppendableExpression) *InsertDataset {
	if sds, ok := from.(*SelectDataset); ok {
		if sds.dialect != GetDialect("default") && id.Dialect() != sds.dialect {
			panic(
				fmt.Errorf(
					"incompatible dialects for INSERT (%q) and SELECT (%q)",
					id.dialect.Dialect(), sds.dialect.Dialect(),
				),
			)
		}
		sds.dialect = id.dialect
	}
	return id.copy(id.clauses.SetFrom(from))
}

// Vals manually set values to insert.
func (id *InsertDataset) Vals(vals ...Vals) *InsertDataset {
	return id.copy(id.clauses.ValsAppend(vals))
}

// ClearVals clears the values.
func (id *InsertDataset) ClearVals() *InsertDataset {
	return id.copy(id.clauses.SetVals(nil))
}

// Rows insert rows. Rows can be a map, goqu.Record or struct.
func (id *InsertDataset) Rows(rows ...interface{}) *InsertDataset {
	return id.copy(id.clauses.SetRows(rows))
}

// ClearRows clears the rows for this insert dataset.
func (id *InsertDataset) ClearRows() *InsertDataset {
	return id.copy(id.clauses.SetRows(nil))
}

// Returning adds a RETURNING clause to the InsertDataset if the adapter supports it.
func (id *InsertDataset) Returning(returning ...interface{}) *InsertDataset {
	return id.copy(id.clauses.SetReturning(exp.NewColumnListExpression(returning...)))
}

// OnConflict adds an (ON CONFLICT/ON DUPLICATE KEY) clause to the InsertDataset if the dialect supports it.
func (id *InsertDataset) OnConflict(conflict exp.ConflictExpression) *InsertDataset {
	return id.copy(id.clauses.SetOnConflict(conflict))
}

// ClearOnConflict clears the on conflict clause.
func (id *InsertDataset) ClearOnConflict() *InsertDataset {
	return id.OnConflict(nil)
}

// Error returns any error that has been set or nil if no error has been set.
func (id *InsertDataset) Error() error {
	return id.err
}

// SetError set an error on the InsertDataset if one has not already been set.
// This error will be returned by a future call to Error or as part of ToSQL.
// This can be used by end users to record errors while building up queries without having to track those separately.
func (id *InsertDataset) SetError(err error) *InsertDataset {
	if id.err == nil {
		id.err = err
	}

	return id
}

// ToSQL generates the default INSERT statement. If Prepared has been called with true then the statement will not be
// interpolated. When using structs you may specify a column to be skipped in the insert, (e.g. id) by
// specifying a goqu tag with `skipinsert`
//
//	type Item struct{
//	   Id   uint32 `db:"id" goqu:"skipinsert"`
//	   Name string `db:"name"`
//	}
//
// rows: variable number arguments of either map[string]interface, Record, struct, or a single slice argument of the
// accepted types.
//
// Errors:
//   - There is no INTO clause
//   - Different row types passed in, all rows must be of the same type
//   - Maps with different numbers of K/V pairs
//   - Rows of different lengths, (i.e. (Record{"name": "a"}, Record{"name": "a", "age": 10})
//   - Error generating SQL
func (id *InsertDataset) ToSQL() (sql string, params []interface{}, err error) {
	return id.insertSQLBuilder().ToSQL()
}

// MustToSQL does the same as ToSQL, but panics instead of returning an error.
func (id *InsertDataset) MustToSQL() (sql string, params []interface{}) {
	var err error
	if sql, params, err = id.insertSQLBuilder().ToSQL(); err != nil {
		panic(err)
	}
	return
}

// AppendSQL appends this InsertDataset's INSERT statement to the sb.SQLBuilder.
// This is used internally when using inserts in CTEs.
func (id *InsertDataset) AppendSQL(b sb.SQLBuilder) {
	if id.err != nil {
		b.SetError(id.err)
		return
	}
	id.dialect.ToInsertSQL(b, id.GetClauses())
}

// GetAs returns the alias value as an identifier expression.
func (id *InsertDataset) GetAs() exp.IdentifierExpression {
	return id.clauses.Alias()
}

// As sets the alias for this InsertDataset.
// This is typically used when using a Dataset as MySQL upsert.
func (id *InsertDataset) As(alias string) *InsertDataset {
	return id.copy(id.clauses.SetAlias(T(alias)))
}

// ReturnsColumns returns whether the InsertDataset has returning columns or not.
func (id *InsertDataset) ReturnsColumns() bool {
	return id.clauses.HasReturning()
}

// Executor generates the INSERT sql, and returns an exec.QueryExecutor struct with the sql set to the INSERT statement.
//
// db.Insert("test").Rows(Record{"name":"Bob"}).Executor().Exec()
func (id *InsertDataset) Executor() exec.QueryExecutor {
	return id.queryFactory.FromSQLBuilder(id.insertSQLBuilder())
}

func (id *InsertDataset) insertSQLBuilder() sb.SQLBuilder {
	buf := sb.NewSQLBuilder(id.isPrepared.Bool())
	if id.err != nil {
		return buf.SetError(id.err)
	}
	id.dialect.ToInsertSQL(buf, id.clauses)
	return buf
}
