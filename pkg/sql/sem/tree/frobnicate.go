package tree

type Frobnicate struct {
	Mode FrobnicateMode
}

var _ Statement = &Frobnicate{}

type FrobnicateMode int

const (
	FrobnicateModeAll FrobnicateMode = iota
	FrobnicateModeCluster
	FrobnicateModeSession
)

func (node *Frobnicate) Format(ctx *FmtCtx) {
	ctx.WriteString("FROBNICATE ")
	switch node.Mode {
	case FrobnicateModeAll:
		ctx.WriteString("ALL")
	case FrobnicateModeCluster:
		ctx.WriteString("CLUSTER")
	case FrobnicateModeSession:
		ctx.WriteString("SESSION")
	}
}


