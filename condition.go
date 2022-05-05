package tdquery

import (
	"errors"
	"fmt"
	"strings"
)

var operatorMap = map[string]struct{}{
	"=":           {},
	">":           {},
	"<":           {},
	">=":          {},
	"<=":          {},
	"!=":          {},
	"<>":          {},
	"IN":          {},
	"IS NULL":     {},
	"IS NOT NULL": {},
	"LIKE":        {},
	"MATCH":       {},
	"BETWEEN":     {},
}

func isValidOperator(operator string) bool {
	_, ok := operatorMap[strings.ToUpper(operator)]
	return ok
}

type Condition struct {
	ColumnName string
	Operator   string
	Value      interface{}
}

func (c *Condition) String() string {
	return c.ColumnName + " " + c.Operator + " ? "
}

func (c *Condition) appendToBuilder(index int, b *strings.Builder, params *[]interface{}) error {
	if b == nil {
		return errors.New("tdquery: condition append to nil builder")
	}
	if !c.IsValid() {
		return ErrInvalidCondition
	}
	if index > 0 {
		b.WriteString(" AND ")
	} else {
		b.WriteString(" WHERE ")
	}
	b.WriteString(c.ColumnName)
	b.WriteRune(' ')
	b.WriteString(c.Operator)
	if c.Value != nil {
		// BETWEEN ? AND ?
		if c.Operator == "BETWEEN" {
			if v, ok := c.Value.([]interface{}); !ok || len(v) != 2 {
				return fmt.Errorf("%w BETWEEN must have 2 values, got %d", ErrInvalidCondition, len(v))
			} else {
				*params = append(*params, v[0], v[1])
			}
			b.WriteString(" ? AND ?")
		} else {
			b.WriteString(" ?")
			*params = append(*params, c.Value)
		}
	}
	return nil
}

func (c *Condition) IsValid() bool {
	return isValidOperator(c.Operator)
}

func NewCondition(column, operator string, value interface{}) *Condition {
	return &Condition{
		ColumnName: column,
		Operator:   operator,
		Value:      value,
	}
}

func Equals(column string, value interface{}) *Condition {
	return &Condition{
		ColumnName: column,
		Operator:   "=",
		Value:      value,
	}
}

func NotEquals(column string, value interface{}) *Condition {
	return &Condition{
		ColumnName: column,
		Operator:   "!=",
		Value:      value,
	}
}

func Greater(column string, value interface{}) *Condition {
	return &Condition{
		ColumnName: column,
		Operator:   ">",
		Value:      value,
	}
}

func Less(column string, value interface{}) *Condition {
	return &Condition{
		ColumnName: column,
		Operator:   "<",
		Value:      value,
	}
}

func GreaterEqual(column string, value interface{}) *Condition {
	return &Condition{
		ColumnName: column,
		Operator:   ">=",
		Value:      value,
	}
}

func LessEqual(column string, value interface{}) *Condition {
	return &Condition{
		ColumnName: column,
		Operator:   "<=",
		Value:      value,
	}
}

func IsNull(column string) *Condition {
	return &Condition{
		ColumnName: column,
		Operator:   "IS NULL",
	}
}

func IsNotNull(column string) *Condition {
	return &Condition{
		ColumnName: column,
		Operator:   "IS NOT NULL",
	}
}
