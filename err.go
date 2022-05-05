package tdquery

import (
	"errors"
	"fmt"
)

var ErrEmptySelect = errors.New("tdquery: select columns is empty")

var ErrEmptyFrom = errors.New("tdquery: table and stable are both empty")

var ErrInvalidCondition = errors.New("tdquery: invalid condition")

var ErrorNoAvailableBroker = errors.New("tdquery: no available broker")

var ErrorInvalidQueryArgsNumber = errors.New("tdquery: query param number not match")

var ErrorInvalidQueryArgs = errors.New("tdquery: invalid query args")

type TDEngineError struct {
	Code    int
	Message string
}

func (e *TDEngineError) Error() string {
	return fmt.Sprintf("tdquery: error from TDengine code: %d, message: %s", e.Code, e.Message)
}
