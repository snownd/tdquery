package tdquery

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const placeholder = "?"

var typeTime = reflect.TypeOf(time.Time{})

// interpolate make prepared statement to right sql "select * from table1 where id=?" value=[1] => "select * from table1 where id=1"
func interpolate(query string, params []interface{}) (string, error) {
	count := strings.Count(query, placeholder)
	if count == 0 {
		return query, nil
	}
	if count != len(params) {
		return "", fmt.Errorf("%w with query: %s, params: %+v", ErrorInvalidQueryArgsNumber, query, params)
	}
	builder := &strings.Builder{}
	argPos := 0
	for {
		index := strings.Index(query, placeholder)
		if index == -1 {
			break
		}
		if strings.HasPrefix(query[index:], placeholder) {
			builder.WriteString(query[:index])
			err := encodePlaceholder(params[argPos], builder)
			if err != nil {
				return "", err
			}
			argPos++
			query = query[index+1:]
			if argPos == count {
				builder.WriteString(query)
			}
		}
	}
	return builder.String(), nil

}

func encodePlaceholder(value interface{}, builder *strings.Builder) error {
	if value == nil {
		builder.WriteString("NULL")
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.String:
		builder.WriteString(encodeString(v.String()))
		return nil
	case reflect.Bool:
		builder.WriteString(encodeBool(v.Bool()))
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		builder.WriteString(strconv.FormatInt(v.Int(), 10))
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		builder.WriteString(strconv.FormatUint(v.Uint(), 10))
		return nil
	case reflect.Float32, reflect.Float64:
		builder.WriteString(strconv.FormatFloat(v.Float(), 'f', -1, 64))
		return nil
	case reflect.Struct:
		if v.Type() == typeTime {
			t := value.(time.Time)
			builder.WriteString(strconv.FormatInt(t.UnixNano()/int64(time.Millisecond), 10))
			return nil
		}
		return fmt.Errorf("%w with param: %+v", ErrorInvalidQueryArgs, v.Interface())
	case reflect.Ptr:
		return encodePlaceholder(v.Elem().Interface(), builder)
	case reflect.Slice, reflect.Array:
		builder.WriteString("(")
		l := v.Len()
		if l > 0 {
			var encoder func(e reflect.Value) string
			f := v.Index(0)
			switch f.Kind() {
			case reflect.String:
				encoder = func(e reflect.Value) string { return encodeString(e.String()) }
			case reflect.Bool:
				encoder = func(e reflect.Value) string { return encodeBool(e.Bool()) }
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				encoder = func(e reflect.Value) string { return strconv.FormatInt(e.Int(), 10) }
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				encoder = func(e reflect.Value) string { return strconv.FormatUint(e.Uint(), 10) }
			default:
				return fmt.Errorf("%w with slice/array param: %+v, unsupported kind", ErrorInvalidQueryArgs, v.Interface())
			}
			for i := 0; i < l; i++ {
				if i > 0 {
					builder.WriteByte(',')
				}
				builder.WriteString(encoder(v.Index(i)))
			}
		}
		builder.WriteString(")")
		return nil
	}
	return ErrorInvalidQueryArgs
}

func encodeString(s string) string {
	return `'` + strings.Replace(s, `'`, `''`, -1) + `'`
}

func encodeBool(b bool) string {
	if b {
		return "TRUE"
	}
	return "FALSE"
}
