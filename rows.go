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
	"github.com/wojnosystems/vsql/vrows"
)

type goRows struct {
	vrows.Rowser
	sqlRows            *sql.Rows
}

// Next calls Next() on the sql.Rows object
func (m *goRows) Next() vrows.Rower {
	if m.sqlRows.Next() {
		return &goRow{
			sqlRows: m.sqlRows,
		}
	}
	return nil
}

// Close cleans up the Rows object, releasing it's object back to the pool. Call this when you're done with your vquery results
func (m *goRows) Close() error {
	return m.sqlRows.Close()
}
