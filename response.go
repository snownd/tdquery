package tdquery

import (
	"time"
)

type queryResultMeta [3]interface{}

type columnType int

const (
	QueryErrCodeTableNotExist = 866
)

const (
	_ columnType = iota
	columnTypeBool
	columnTypeTinyInt
	columnTypeSmallInt
	columnTypeInt
	columnTypeBigInt
	columnTypeFloat
	columnTypeDouble
	columnTypeBinary
	columnTypeTimestamp
	columnTypeNchar
)

func (m queryResultMeta) GetColumnType() columnType {
	return columnType(m[1].(float64))
}

func (m queryResultMeta) GetColumnName() string {
	return m[0].(string)
}

type rawQueryResult struct {
	Status     string            `json:"status"`
	Code       int               `json:"code"`
	Desc       string            `json:"desc,omitempty"`
	Head       []string          `json:"head"`
	ColumnMeta []queryResultMeta `json:"column_meta"`
	Data       [][]interface{}   `json:"data"`
	Rows       int               `json:"rows"`
}

type QueryResult struct {
	Code    int                      `json:"code"`
	Message string                   `json:"message,omitempty"`
	SQL     string                   `json:"sql,omitempty"`
	Data    []map[string]interface{} `json:"data"`
	Rows    int                      `json:"rows"`
	// 单位毫秒
	Cost int `json:"cost"`
}

func NewQueryResult(raw *rawQueryResult, sql string, cost time.Duration) *QueryResult {
	r := &QueryResult{
		Code:    raw.Code,
		Message: raw.Desc,
		Cost:    int(cost / time.Millisecond),
		Data:    make([]map[string]interface{}, 0, len(raw.Data)),
	}
	// 拦截表不存在错误,返回空值
	if raw.Code == QueryErrCodeTableNotExist {
		return r
	}
	meta := raw.ColumnMeta

	for _, row := range raw.Data {
		mapedValue := make(map[string]interface{})
		for i, rowColumn := range row {
			switch meta[i].GetColumnType() {
			case columnTypeBool:
				mapedValue[meta[i].GetColumnName()] = rowColumn.(float64) == 1
			default:
				mapedValue[meta[i].GetColumnName()] = rowColumn
			}
		}
		r.Data = append(r.Data, mapedValue)
	}

	return r
}
