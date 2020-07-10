package workflow

type dependency struct {
	requires []*Task
}

func cloneTasks(src []*Task) []*Task {
	if len(src) == 0 {
		return nil
	}
	dst := make([]*Task, len(src))
	copy(dst, src)
	return dst
}

func (dep *dependency) clone() dependency {
	return dependency{
		requires: cloneTasks(dep.requires),
	}
}

func (dep dependency) walk(fn func(*Task)) {
	for _, task := range dep.requires {
		fn(task)
	}
}

func (dep dependency) isSatisfy(wCtx *workflowContext) bool {
	for _, req := range dep.requires {
		reqCtx, ok := wCtx.contexts[req]
		if ok && (!reqCtx.ended || reqCtx.err != nil) {
			return false
		}
	}
	return true
}
