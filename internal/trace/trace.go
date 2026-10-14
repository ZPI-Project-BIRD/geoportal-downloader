package trace

import (
	"context"

	"github.com/google/uuid"
)

type contextKey struct{}

func NewID() string {
	return uuid.NewString()
}

func WithID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, contextKey{}, id)
}

func ID(ctx context.Context) string {
	id, _ := ctx.Value(contextKey{}).(string)
	return id
}
