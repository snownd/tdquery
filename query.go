package tdquery

import (
	"context"
	"strings"
)

type QueryBuilder struct {
	c        *Client
	builder  strings.Builder
	database string
	sTable   string
	tables   []string
	query    string
}

func (b *QueryBuilder) Build() (string, error) {
	return b.builder.String(), nil
}

func (b *QueryBuilder) FromSTable(stable string) *QueryBuilder {
	b.sTable = stable
	return b
}

func (b *QueryBuilder) FromTables(tables ...string) *QueryBuilder {
	b.tables = append(b.tables, tables...)
	return b
}

func (b *QueryBuilder) UseDatabase(db string) *QueryBuilder {
	b.database = db
	return b
}

func (b *QueryBuilder) GetRaw(ctx context.Context, sql string, params ...interface{}) (*QueryResult, error) {
	return b.c.Query(ctx, sql, params...)
}
