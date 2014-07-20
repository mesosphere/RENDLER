package rendler

import (
	"fmt"
)

type Edge struct {
	From string
	To   string
}

func (e Edge) String() string {
	return fmt.Sprintf("(%s, %s)", e.From, e.To)
}

