package redislock

import (
	"context"
)

const kThreadId = "k_thread_id"

func WithThreadId(ctx context.Context, threadId string) context.Context {
	return context.WithValue(ctx, kThreadId, threadId)
}

func getThreadId(ctx context.Context) string {
	if v, ok := ctx.Value(kThreadId).(string); ok {
		return v
	}
	panic("invalid context")
}
