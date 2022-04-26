package sqlextratxnstate

import "github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"

// ExtraTxnState holds state to initialize an InternalExecutor with.
type ExtraTxnState struct {
	Descs *descs.Collection
}
