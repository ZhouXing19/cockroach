package sql

import (
	"bytes"
	"context"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
	"math/rand"
)

func (p *planner) Frobnicate(ctx context.Context, stmt *tree.Frobnicate) (planNode, error) {
	switch stmt.Mode {
	case tree.FrobnicateModeSession:
		p.randomizeSessionSettings()
	default:
		return nil, errors.AssertionFailedf("Unhandled FROBNICATE mode %v!", stmt.Mode)
	}

	return newZeroNode(nil /* columns */), nil
}

var distSQLOptions = []string{"off", "on", "auto", "always"}

func randomMode() sessiondata.DistSQLExecMode {
	i := rand.Int() % len(distSQLOptions)
	mode, _ := sessiondata.DistSQLExecModeFromString(distSQLOptions[i])
	return mode;
}

func randomName() string {
	length := 10 + rand.Int() % 10
	buf := bytes.NewBuffer(make([]byte, 0, length))

	for i := 0; i < length; i++ {
		ch := 'a' + rune(rand.Int() % 26)
		buf.WriteRune(ch)
	}

	return buf.String()
}

func (p *planner) randomizeSessionSettings() {
	mutator := p.sessionDataMutator

	mutator.SetDistSQLMode(randomMode())
	mutator.SetApplicationName(randomName())
}

