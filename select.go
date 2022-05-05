package tdquery

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"
)

var periodRegexp = regexp.MustCompile("^[0-9]+[BUASMHDWNY]$")

func IsValidPeriod(period string) bool {
	return periodRegexp.MatchString(period)
}

type Order int

const (
	ASC Order = iota
	DESC
)

func (o Order) String() string {
	switch o {
	case ASC:
		return "ASC"
	case DESC:
		return "DESC"
	default:
		panic("tdquery: unknown order")
	}
}

type orderBy struct {
	columns []string
	order   Order
}

type Interval struct {
	period string
	offset int
}

func (i *Interval) WithOffset(n int) *Interval {
	i.offset = n
	return i
}

func NewInterval(period string) *Interval {
	p := strings.ToUpper(period)
	if !IsValidPeriod(p) {
		panic("tdquery: invalid period")
	}
	return &Interval{period: p}
}

func (i *Interval) String() string {
	s := i.period
	if i.offset > 0 {
		s = s + ", " + strconv.Itoa(i.offset)
	}
	return s
}

type FillType int

const (
	FillTypeValue FillType = iota + 1
	FillTypePrevious
	FillTypeNull
	FillTypeLinear
	FillTypeNext
)

func (f FillType) String() string {
	switch f {
	case FillTypeValue:
		return "VALUE"
	case FillTypePrevious:
		return "PREV"
	case FillTypeNull:
		return "NULL"
	case FillTypeLinear:
		return "LINEAR"
	case FillTypeNext:
		return "NEXT"
	default:
		panic("tdquery: unknown fill type")
	}
}

type Fill struct {
	value    string
	fillType FillType
}

func (f *Fill) String() string {
	if f.fillType == FillTypeValue {
		return "VALUE, " + f.value
	}
	return f.fillType.String()
}

func FillValue(value string) *Fill {
	return &Fill{value: value, fillType: FillTypeValue}
}

func FillPrev() *Fill {
	return &Fill{fillType: FillTypePrevious}
}

func FillNull() *Fill {
	return &Fill{fillType: FillTypeNull}
}

func FillLinear() *Fill {
	return &Fill{fillType: FillTypeLinear}
}

func FillNext() *Fill {
	return &Fill{fillType: FillTypeNext}
}

type Select struct {
	ColumnName string
	Alias      string
}

type SelectQueryBuilder struct {
	QueryBuilder
	selects  []Select
	interval *Interval
	slimit   int
	soffset  int
	limit    int
	offset   int
	orderBy  *orderBy
	where    []*Condition
	groupby  []string
	fill     *Fill
	subQuery *SelectQueryBuilder
	params   []interface{}
}

func (b *SelectQueryBuilder) Select(selects ...Select) *SelectQueryBuilder {
	b.selects = append(b.selects, selects...)
	return b
}
func (b *SelectQueryBuilder) AddSelect(s Select) *SelectQueryBuilder {
	b.selects = append(b.selects, s)
	return b
}

func (b *SelectQueryBuilder) SelectColumn(columnName string) *SelectQueryBuilder {
	return b.SelectColumnWithAlias(columnName, "")
}

func (b *SelectQueryBuilder) SelectColumnWithAlias(columnName, alias string) *SelectQueryBuilder {
	return b.AddSelect(Select{ColumnName: columnName, Alias: alias})
}

func (b *SelectQueryBuilder) SelectAll() *SelectQueryBuilder {
	return b.SelectColumn("*")
}

func (b *SelectQueryBuilder) FromSTable(stable string) *SelectQueryBuilder {
	b.QueryBuilder.FromSTable(stable)
	return b
}

func (b *SelectQueryBuilder) FromTables(tables ...string) *SelectQueryBuilder {
	b.tables = append(b.tables, tables...)
	return b
}

func (b *SelectQueryBuilder) FromSubQuery(subQuery *SelectQueryBuilder) *SelectQueryBuilder {
	b.subQuery = subQuery
	return b
}

func (b *SelectQueryBuilder) UseDatabase(db string) *SelectQueryBuilder {
	b.QueryBuilder.UseDatabase(db)
	return b
}

func (b *SelectQueryBuilder) Where(conditions ...*Condition) *SelectQueryBuilder {
	b.where = append(b.where, conditions...)
	return b
}

func (b *SelectQueryBuilder) AndWhere(conditions ...*Condition) *SelectQueryBuilder {
	return b.Where(conditions...)
}

// WithTimeScope generate sql with BETWEEN: _co between start and end
func (b *SelectQueryBuilder) WithTimeScope(start, end time.Time) *SelectQueryBuilder {
	return b.Where(NewCondition("_c0", "BETWEEN", []interface{}{start, end}))
}

// OrderBy only support ASC or DESC with time column
func (b *SelectQueryBuilder) OrderBy(columns []string, order Order) *SelectQueryBuilder {
	b.orderBy = &orderBy{
		columns: columns,
		order:   order,
	}
	return b
}

// order by time column with DESC order
func (b *SelectQueryBuilder) Desc() *SelectQueryBuilder {
	return b.OrderBy([]string{"_c0"}, DESC)
}

// order by time column with ASC order
func (b *SelectQueryBuilder) Asc() *SelectQueryBuilder {
	return b.OrderBy([]string{"_c0"}, ASC)
}

func (b *SelectQueryBuilder) Limit(limit int) *SelectQueryBuilder {
	b.limit = limit
	return b
}

func (b *SelectQueryBuilder) Offset(offset int) *SelectQueryBuilder {
	b.offset = offset
	return b
}

func (b *SelectQueryBuilder) Interval(interval *Interval) *SelectQueryBuilder {
	b.interval = interval
	return b
}

func (b *SelectQueryBuilder) GroupBy(columns ...string) *SelectQueryBuilder {
	b.groupby = append(b.groupby, columns...)
	return b
}

func (b *SelectQueryBuilder) SLimit(limit int) *SelectQueryBuilder {
	b.slimit = limit
	return b
}

func (b *SelectQueryBuilder) SOffset(offset int) *SelectQueryBuilder {
	b.soffset = offset
	return b
}

func (b *SelectQueryBuilder) Fill(f *Fill) *SelectQueryBuilder {
	b.fill = f
	return b
}

func (b *SelectQueryBuilder) Build() (string, error) {
	if b.builder.Len() == 0 {
		if err := b.buildSQL(); err != nil {
			return "", err
		}
	}
	return b.QueryBuilder.Build()
}

func (b *SelectQueryBuilder) buildSQL() error {
	if len(b.selects) == 0 {
		return ErrEmptySelect
	}
	b.builder.WriteString("SELECT ")
	for i, s := range b.selects {
		if i > 0 {
			b.builder.WriteString(", ")
		}
		b.builder.WriteString(s.ColumnName)
		if s.Alias != "" {
			b.builder.WriteString(" AS \"")
			b.builder.WriteString(s.Alias)
			b.builder.WriteRune('"')
		}
	}
	b.builder.WriteString(" FROM ")
	if b.QueryBuilder.sTable != "" {
		if b.QueryBuilder.database != "" {
			b.builder.WriteString(b.database)
			b.builder.WriteRune('.')
		}
		b.builder.WriteString(b.QueryBuilder.sTable)
	} else if len(b.QueryBuilder.tables) > 0 {
		for i, table := range b.QueryBuilder.tables {
			if i > 0 {
				b.builder.WriteString(", ")
			}
			if b.QueryBuilder.database != "" {
				b.builder.WriteString(b.database)
				b.builder.WriteRune('.')
			}
			b.builder.WriteString(table)
		}
	} else if b.subQuery != nil {
		subSql, err := b.subQuery.Build()
		if err != nil {
			return err
		}
		b.builder.WriteRune('(')
		b.builder.WriteString(subSql)
		b.builder.WriteRune(')')
	} else {
		return ErrEmptyFrom
	}
	builder := &b.builder
	for i, c := range b.where {
		if err := c.appendToBuilder(i, builder, &b.params); err != nil {
			return err
		}
	}
	if b.interval != nil {
		b.builder.WriteString(" INTERVAL(")
		b.builder.WriteString(b.interval.String())
		b.builder.WriteRune(')')
	}

	if b.fill != nil {
		b.builder.WriteString(" FILL(")
		b.builder.WriteString(b.fill.String())
		b.builder.WriteRune(')')
	}

	if len(b.groupby) > 0 {
		b.builder.WriteString(" GROUP BY ")
		for i, g := range b.groupby {
			if i > 0 {
				b.builder.WriteString(", ")
			}
			b.builder.WriteString(g)
		}
	}

	if b.orderBy != nil {
		b.builder.WriteString(" ORDER BY ")
		for i, o := range b.orderBy.columns {
			if i > 0 {
				b.builder.WriteString(", ")
			}
			b.builder.WriteString(o)
		}
		b.builder.WriteString(" ")
		b.builder.WriteString(b.orderBy.order.String())
	}

	if b.slimit > 0 {
		b.builder.WriteString(" SLIMIT ")
		b.builder.WriteString(strconv.Itoa(b.slimit))
		if b.soffset > 0 {
			b.builder.WriteString(" SOFFSET ")
			b.builder.WriteString(strconv.Itoa(b.soffset))
		}
	}

	if b.limit > 0 {
		b.builder.WriteString(" LIMIT ")
		b.builder.WriteString(strconv.Itoa(b.limit))
		if b.offset > 0 {
			b.builder.WriteString(" OFFSET ")
			b.builder.WriteString(strconv.Itoa(b.offset))
		}
	}
	return nil
}

func (s *SelectQueryBuilder) GetRaw(ctx context.Context) (*QueryResult, error) {
	sql, err := s.Build()
	if err != nil {
		return nil, err
	}
	return s.QueryBuilder.GetRaw(ctx, sql, s.params...)
}

func (s *SelectQueryBuilder) GetResult(ctx context.Context, v interface{}) error {
	raw, err := s.GetRaw(ctx)
	if err != nil {
		return err
	}
	if raw.Code != 0 {
		return &TDEngineError{Code: raw.Code, Message: raw.Message}
	}
	return mapstructure.Decode(raw.Data, v)
}
