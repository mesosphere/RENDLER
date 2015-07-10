package rendler

import (
	"fmt"
)

// Edge represents a directional link between two URLs
type Edge struct {
	From string
	To   string
}

func (e Edge) String() string {
	return fmt.Sprintf("(%s, %s)", e.From, e.To)
}
