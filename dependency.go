package workflow

type dependType int

const (
	onComplete dependType = iota
	onStart
	onEnd
)

type dependency map[*Task]dependType

func (dep dependency) clone() dependency {
	dst := make(dependency, len(dep))
	for task, typ := range dep {
		dst[task] = typ
	}
	return dst
}

func (dep dependency) walk(fn func(*Task)) {
	for task := range dep {
		fn(task)
	}
}

// when adds dependency to tasks
func (dep dependency) when(typ dependType, tasks ...*Task) dependency {
	for _, task := range tasks {
		dep[task] = typ
	}
	return dep
}

func (dep dependency) isSatisfy(wCtx *workflowContext) bool {
	for task, typ := range dep {
		reqCtx, ok := wCtx.contexts[task]
		switch typ {
		case onComplete:
			if ok && (!reqCtx.ended || reqCtx.err != nil) {
				return false
			}
		case onStart:
			if ok && !reqCtx.started {
				return false
			}
		case onEnd:
			if ok && !reqCtx.ended {
				return false
			}
		}
	}
	return true
}
