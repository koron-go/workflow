package workflow

// Logger defines interface to log start/end of tasks.
type Logger interface {
	Printf(format string, v ...interface{})
}

type discardLogger struct{}

func (discardLogger) Printf(string, ...interface{}) {}
