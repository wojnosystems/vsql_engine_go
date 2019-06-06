//Copyright 2019 Chris Wojno
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
// documentation files (the "Software"), to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
// Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
// OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package vsql_engine_go

import (
	"context"
	"database/sql"
	"github.com/wojnosystems/vsql/interpolation_strategy"
	"github.com/wojnosystems/vsql/vparam"
	"github.com/wojnosystems/vsql/vresult"
	"github.com/wojnosystems/vsql/vrows"
	"github.com/wojnosystems/vsql/vstmt"
)

type queryExecTransaction struct {
	goTransaction       *sql.Tx
	interpolateStrategy interpolation_strategy.InterpolateStrategy
}

func newQueryExecTransaction(goTransaction *sql.Tx, interpolateStrategy interpolation_strategy.InterpolateStrategy) *queryExecTransaction {
	return &queryExecTransaction{
		goTransaction:       goTransaction,
		interpolateStrategy: interpolateStrategy,
	}
}

// Commit ends a transaction by persisting the requested changes
func (q *queryExecTransaction) Commit() error {
	return q.goTransaction.Commit()
}

// Rollback ends a transaction by not persisting the changes made via queries while within the transaction
func (q *queryExecTransaction) Rollback() error {
	return q.goTransaction.Rollback()
}

func (q *queryExecTransaction) Query(ctx context.Context, query vparam.Queryer) (rows vrows.Rowser, err error) {
	queryString, values, err := query.Interpolate(query.SQLQueryUnInterpolated(),q.interpolateStrategy)
	if err != nil {
		return nil, err
	}
	sqlRows, err := q.goTransaction.QueryContext(ctx, queryString, values...)
	if err != nil {
		return nil, err
	}
	r := &goRows{
		sqlRows: sqlRows,
	}
	return r, err
}
func (q *queryExecTransaction) Insert(ctx context.Context, query vparam.Queryer) (result vresult.InsertResulter, err error) {
	queryString, values, err := query.Interpolate(query.SQLQueryUnInterpolated(),q.interpolateStrategy)
	if err != nil {
		return nil, err
	}
	res, err := q.goTransaction.ExecContext(ctx, queryString, values...)
	if err != nil {
		return nil, err
	}
	return &goInsertResult{result: res,}, err
}
func (q *queryExecTransaction) Exec(ctx context.Context, query vparam.Queryer) (result vresult.Resulter, err error) {
	return q.Insert(ctx, query)
}
func (q *queryExecTransaction) Prepare(ctx context.Context, query vparam.Queryer) (stmt vstmt.Statementer, err error) {
	goStmt, err := q.goTransaction.PrepareContext(ctx,query.SQLQueryInterpolated(q.interpolateStrategy))
	if err != nil {
		return
	}
	stmtWrapper := newStatement( goStmt, q.interpolateStrategy )
	stmtWrapper.originalQuery = query
	return stmtWrapper, err
}
