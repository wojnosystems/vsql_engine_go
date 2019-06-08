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
	"database/sql"
	"github.com/wojnosystems/vsql/interpolation_strategy"
	"github.com/wojnosystems/vsql_engine"
)

//Injects all of the middleware required to perform database queries :) Yes, the database layer interpolateStrategyFactory middleware, too. Incept'ed!
func InstallMulti(engine vsql_engine.MultiTXer, db *sql.DB, factory interpolation_strategy.InterpolationStrategyFactory) {
	panic("not implemented yet")
	//engine.BeginNestedMW().Prepend(func(ctx context.Context, c engine_context.NestedBeginner) {
	//	tx, err := db.BeginTx(ctx, c.TxOptions().ToTxOptions())
	//	c.SetQueryExecNestedTransactioner(newQueryExecNestedTransaction(tx, factory()))
	//	if err != nil {
	//		c.SetError(err)
	//		return
	//	}
	//	c.Next(ctx)
	//})
	//engine.StatementPrepareMW().Prepend(func(ctx context.Context, c engine_context.Preparer) {
	//	goStmt, err := db.PrepareContext(ctx, c.Query().SQLQueryInterpolated(factory()))
	//	if err != nil {
	//		c.SetError(err)
	//		return
	//	}
	//	stmtWrap := newStatement(goStmt, factory())
	//	stmtWrap.originalQuery = c.Query()
	//	c.SetStatement(stmtWrap)
	//	c.Next(ctx)
	//})
	//engine.QueryMW().Prepend(func(ctx context.Context, c engine_context.Queryer) {
	//	sqlQ, args, err := c.Query().Interpolate(c.Query().SQLQueryUnInterpolated(), factory())
	//	if err != nil {
	//		c.SetError(err)
	//		return
	//	}
	//	goRowsOut, err := db.QueryContext(ctx, sqlQ, args...)
	//	if err != nil {
	//		c.SetError(err)
	//		return
	//	}
	//	rowsWrap := &goRows{
	//		sqlRows: goRowsOut,
	//	}
	//	c.SetRows(rowsWrap)
	//	c.Next(ctx)
	//})
	//engine.InsertQueryMW().Prepend(func(ctx context.Context, c engine_context.Inserter) {
	//	sqlQ, args, err := c.Query().Interpolate(c.Query().SQLQueryUnInterpolated(), factory())
	//	if err != nil {
	//		c.SetError(err)
	//		return
	//	}
	//	goResOut, err := db.ExecContext(ctx, sqlQ, args...)
	//	if err != nil {
	//		c.SetError(err)
	//		return
	//	}
	//	resultWrap := &goInsertResult{
	//		result: goResOut,
	//	}
	//	c.SetInsertResult(resultWrap)
	//	c.Next(ctx)
	//})
	//engine.ExecQueryMW().Prepend(func(ctx context.Context, c engine_context.Execer) {
	//	sqlQ, args, err := c.Query().Interpolate(c.Query().SQLQueryUnInterpolated(), factory())
	//	if err != nil {
	//		c.SetError(err)
	//		return
	//	}
	//	goResOut, err := db.ExecContext(ctx, sqlQ, args...)
	//	if err != nil {
	//		c.SetError(err)
	//		return
	//	}
	//	resultWrap := &goInsertResult{
	//		result: goResOut,
	//	}
	//	c.SetResult(resultWrap)
	//	c.Next(ctx)
	//})
	//engine.PingMW().Prepend(func(ctx context.Context, c engine_context.Er) {
	//	err := db.Ping()
	//	if err != nil {
	//		c.SetError(err)
	//		return
	//	}
	//	c.Next(ctx)
	//})
	//engine.StatementCloseMW().Prepend(func(ctx context.Context, c engine_context.StatementCloser) {
	//	err := c.Statement().Close()
	//	if err != nil {
	//		c.SetError(err)
	//		return
	//	}
	//	c.Next(ctx)
	//})
	//engine.StatementQueryMW().Prepend(func(ctx context.Context, c engine_context.StatementQueryer) {
	//	goRowsOut, err := c.Statement().Query(ctx, c.Parameterer())
	//	if err != nil {
	//		c.SetError(err)
	//		return
	//	}
	//	c.SetRows(goRowsOut)
	//	c.Next(ctx)
	//})
	//engine.StatementInsertQueryMW().Prepend(func(ctx context.Context, c engine_context.StatementInsertQueryer) {
	//	goInsertResult, err := c.Statement().Insert(ctx, c.Parameterer())
	//	if err != nil {
	//		c.SetError(err)
	//		return
	//	}
	//	c.SetInsertResult(goInsertResult)
	//	c.Next(ctx)
	//})
	//engine.StatementExecQueryMW().Prepend(func(ctx context.Context, c engine_context.StatementExecQueryer) {
	//	goResult, err := c.Statement().Exec(ctx, c.Parameterer())
	//	if err != nil {
	//		c.SetError(err)
	//		return
	//	}
	//	c.SetResult(goResult)
	//	c.Next(ctx)
	//})
	//engine.CommitMW().Prepend(func(ctx context.Context, c engine_context.Beginner) {
	//	err := c.QueryExecTransactioner().Commit()
	//	if err != nil {
	//		c.SetError(err)
	//		return
	//	}
	//	c.Next(ctx)
	//})
	//engine.RollbackMW().Prepend(func(ctx context.Context, c engine_context.Beginner) {
	//	err := c.QueryExecTransactioner().Rollback()
	//	if err != nil {
	//		c.SetError(err)
	//		return
	//	}
	//	c.Next(ctx)
	//})
	//engine.RowsNextMW().Prepend(func(ctx context.Context, c engine_context.RowsNexter) {
	//	nextRow := c.Rows().Next()
	//	c.SetRow(nextRow)
	//	c.Next(ctx)
	//})
	//engine.RowsCloseMW().Prepend(func(ctx context.Context, c engine_context.Rowser) {
	//	err := c.Rows().Close()
	//	if err != nil {
	//		c.SetError(err)
	//		return
	//	}
	//	c.Next(ctx)
	//})
	//engine.ConnCloseMW().Prepend(func(ctx context.Context, c engine_context.Er) {
	//	err := db.Close()
	//	if err != nil {
	//		c.SetError(err)
	//		return
	//	}
	//	c.Next(ctx)
	//})
}
