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
)

type statement struct {
	stmt                       *sql.Stmt
	interpolateStrategyFactory interpolation_strategy.InterpolationStrategyFactory
	originalQuery              vparam.Queryer
}

func newStatement(s *sql.Stmt, interpolateStrategyFactory interpolation_strategy.InterpolationStrategyFactory) *statement {
	return &statement{
		stmt:                       s,
		interpolateStrategyFactory: interpolateStrategyFactory,
	}
}

func (s *statement) Close() error {
	return s.stmt.Close()
}

func (s *statement) Query(ctx context.Context, query vparam.Parameterer) (rows vrows.Rowser, err error) {
	_, values, err := query.Interpolate(s.originalQuery.SQLQueryUnInterpolated(), s.interpolateStrategyFactory())
	if err != nil {
		return nil, err
	}
	sqlRows, err := s.stmt.QueryContext(ctx, values...)
	if err != nil {
		return nil, err
	}
	r := &goRows{
		sqlRows: sqlRows,
	}
	return r, err
}

func (s *statement) Insert(ctx context.Context, query vparam.Parameterer) (result vresult.InsertResulter, err error) {
	_, values, err := query.Interpolate(s.originalQuery.SQLQueryUnInterpolated(), s.interpolateStrategyFactory())
	if err != nil {
		return nil, err
	}
	res, err := s.stmt.ExecContext(ctx, values...)
	if err != nil {
		return nil, err
	}
	return &goInsertResult{result: res}, err
}

func (s *statement) Exec(ctx context.Context, query vparam.Parameterer) (result vresult.Resulter, err error) {
	return s.Insert(ctx, query)
}
