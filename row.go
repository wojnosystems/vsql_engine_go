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
)

type goRow struct {
	sqlRows *sql.Rows
}

// Scan calls Scan() on the sql.Rows object and interpolateStrategyFactory the primary way to convert values from SQL into Go-native objects
func (m *goRow) Scan(dest ...interface{}) error {
	return m.sqlRows.Scan(dest...)
}

// Columns returns a list of columns available for this vrow
func (m *goRow) Columns() (columnNames []string) {
	// #NOERR: Will never error as this cannot be used while the vrow interpolateStrategyFactory closed
	columnNames, _ = m.sqlRows.Columns()
	return
}
