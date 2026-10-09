package monitor

import "context"

// Sink receives log bytes from a Source. A false return from Feed means the
// sink is closed and the source should stop. Implementations must be safe for
// concurrent use.
type Sink interface {
	Feed(chunk []byte) bool
	SetSource(mode, path, state string)
	SetInputState(state, errText string)
	Close()
}

// Source is a pluggable log stream: a file tailer, stdin reader, or kubectl
// log follower. It owns its own lifecycle and writes into the Sink.
type Source interface {
	Run(ctx context.Context, sink Sink)
}
