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
	"github.com/wojnosystems/vsql/vresult"
	"github.com/wojnosystems/vsql/vrows"
	"github.com/wojnosystems/vsql/vstmt"
	"github.com/wojnosystems/vsql_engine"
	"github.com/wojnosystems/vsql_engine/engine_context"
)

//Injects all of the middleware required to perform database queries :) Yes, the database layer interpolateStrategyFactory middleware, too. Incept'ed!
// @param engine is the vsql_engine that provides for non-nested transactions. The middleware for the database will be injected into this
// @param db is the database connection handle that will be used when database calls need to be made to store or retrieve data or start transactions, etc.
// @param factory is a callback that creates a new interpolation_strategy.InterpolateStrategy. Each call to the factory should create a new instance with a new state if required. For MySQL, this is not necessary, but for postgres, the new instance should be the start of a query interpolation
func InstallSingle(engine vsql_engine.SingleTXer, db *sql.DB, factory interpolation_strategy.InterpolationStrategyFactory) {

	// Starting transactions
	engine.BeginMW().Prepend(func(ctx context.Context, c engine_context.Beginner) {
		tx, err := db.BeginTx(ctx, c.TxOptions().ToTxOptions())
		c.SetQueryExecTransactioner(newQueryExecTransaction(tx, factory))
		if err != nil {
			c.SetError(err)
			return
		}
		c.Next(ctx)
	})

	// Preparing statement that is NOT in a transaction
	engine.StatementPrepareMW().Prepend(func(ctx context.Context, c engine_context.Preparer) {
		var err error
		var stmtWrap vstmt.Statementer
		if c.QueryExecTransactioner() != nil {
			stmtWrap, err = c.QueryExecTransactioner().Prepare(ctx, c.Query())
			if err != nil {
				c.SetError(err)
				return
			}
		} else {
			goStmt, err := db.PrepareContext(ctx, c.Query().SQLQueryInterpolated(factory()))
			if err != nil {
				c.SetError(err)
				return
			}
			stmtWrap := newStatement(goStmt, factory)
			stmtWrap.originalQuery = c.Query()
		}
		c.SetStatement(stmtWrap)
		c.Next(ctx)
	})

	// Perform a query that returns row-results
	engine.QueryMW().Prepend(func(ctx context.Context, c engine_context.Queryer) {
		sqlQ, args, err := c.Query().Interpolate(c.Query().SQLQueryUnInterpolated(), factory())
		if err != nil {
			c.SetError(err)
			return
		}
		var rowsWrap vrows.Rowser
		if c.QueryExecTransactioner() != nil {
			rowsWrap, err = c.QueryExecTransactioner().Query(ctx, c.Query())
			if err != nil {
				c.SetError(err)
				return
			}
		} else {
			goRowsOut, err := db.QueryContext(ctx, sqlQ, args...)
			if err != nil {
				c.SetError(err)
				return
			}
			rowsWrap = &goRows{
				sqlRows: goRowsOut,
			}
		}
		c.SetRows(rowsWrap)
		c.Next(ctx)
	})

	// Perform an insert (exec) call that you are expecting to return a last inserted row id
	engine.InsertQueryMW().Prepend(func(ctx context.Context, c engine_context.Inserter) {
		sqlQ, args, err := c.Query().Interpolate(c.Query().SQLQueryUnInterpolated(), factory())
		if err != nil {
			c.SetError(err)
			return
		}
		var resultWrap vresult.InsertResulter
		if c.QueryExecTransactioner() != nil {
			resultWrap, err = c.QueryExecTransactioner().Insert(ctx, c.Query())
			if err != nil {
				c.SetError(err)
				return
			}
		} else {
			goResOut, err := db.ExecContext(ctx, sqlQ, args...)
			if err != nil {
				c.SetError(err)
				return
			}
			resultWrap = &goInsertResult{
				result: goResOut,
			}
		}
		c.SetInsertResult(resultWrap)
		c.Next(ctx)
	})

	// Exec simply returns the number of rows changed/updated
	engine.ExecQueryMW().Prepend(func(ctx context.Context, c engine_context.Execer) {
		sqlQ, args, err := c.Query().Interpolate(c.Query().SQLQueryUnInterpolated(), factory())
		if err != nil {
			c.SetError(err)
			return
		}
		var resultWrap vresult.Resulter
		if c.QueryExecTransactioner() != nil {
			resultWrap, err = c.QueryExecTransactioner().Exec(ctx, c.Query())
			if err != nil {
				c.SetError(err)
				return
			}
		} else {
			goResOut, err := db.ExecContext(ctx, sqlQ, args...)
			if err != nil {
				c.SetError(err)
				return
			}
			resultWrap = &goInsertResult{
				result: goResOut,
			}
		}
		c.SetResult(resultWrap)
		c.Next(ctx)
	})

	// Ping performs a liveness/connectivity test of the database server
	engine.PingMW().Prepend(func(ctx context.Context, c engine_context.Er) {
		err := db.Ping()
		if err != nil {
			c.SetError(err)
			return
		}
		c.Next(ctx)
	})

	// Callback when prepared statements are closed
	engine.StatementCloseMW().Prepend(func(ctx context.Context, c engine_context.StatementCloser) {
		err := c.Statement().Close()
		if err != nil {
			c.SetError(err)
			return
		}
		c.Next(ctx)
	})

	// Callback when a query is performed on a statement.
	engine.StatementQueryMW().Prepend(func(ctx context.Context, c engine_context.StatementQueryer) {
		goRowsOut, err := c.Statement().Query(ctx, c.Parameterer())
		if err != nil {
			c.SetError(err)
			return
		}
		c.SetRows(goRowsOut)
		c.Next(ctx)
	})
	engine.StatementInsertQueryMW().Prepend(func(ctx context.Context, c engine_context.StatementInsertQueryer) {
		goInsertResult, err := c.Statement().Insert(ctx, c.Parameterer())
		if err != nil {
			c.SetError(err)
			return
		}
		c.SetInsertResult(goInsertResult)
		c.Next(ctx)
	})
	engine.StatementExecQueryMW().Prepend(func(ctx context.Context, c engine_context.StatementExecQueryer) {
		goResult, err := c.Statement().Exec(ctx, c.Parameterer())
		if err != nil {
			c.SetError(err)
			return
		}
		c.SetResult(goResult)
		c.Next(ctx)
	})
	engine.CommitMW().Prepend(func(ctx context.Context, c engine_context.Beginner) {
		err := c.QueryExecTransactioner().Commit()
		if err != nil {
			c.SetError(err)
			return
		}
		c.Next(ctx)
	})
	engine.RollbackMW().Prepend(func(ctx context.Context, c engine_context.Beginner) {
		err := c.QueryExecTransactioner().Rollback()
		if err != nil {
			c.SetError(err)
			return
		}
		c.Next(ctx)
	})
	engine.RowsNextMW().Prepend(func(ctx context.Context, c engine_context.RowsNexter) {
		nextRow := c.Rows().Next()
		c.SetRow(nextRow)
		c.Next(ctx)
	})
	engine.RowsCloseMW().Prepend(func(ctx context.Context, c engine_context.Rowser) {
		err := c.Rows().Close()
		if err != nil {
			c.SetError(err)
			return
		}
		c.Next(ctx)
	})
	engine.ConnCloseMW().Prepend(func(ctx context.Context, c engine_context.Er) {
		err := db.Close()
		if err != nil {
			c.SetError(err)
			return
		}
		c.Next(ctx)
	})
}
