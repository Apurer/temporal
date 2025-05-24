package stamp

import (
	"context"
	"fmt"

	"go.temporal.io/server/common/future"
)

type (
	Future[T any] struct {
		testEnv testEnv
		f       *future.FutureImpl[T]
		cancel  context.CancelFunc
	}
	getter[T any] interface {
		get() (T, bool)
	}
)

func newFuture[T any](
	env testEnv,
	fn func() (T, error),
) Future[T] {
	// TODO: configurable timeout
	ctx, cancel := context.WithCancel(env.Context(defaultWaiterPolling))
	fut := Future[T]{
		testEnv: env,
		f:       future.NewFuture[T](),
		cancel:  cancel,
	}

	go func() {
		select {
		case <-ctx.Done():
			return
		default:
			res, err := fn()
			select {
			case <-ctx.Done():
				return // ignore result if cancelled
			default:
				fut.f.Set(res, err)
			}
		}
	}()

	return fut
}

// TODO: replace with top-level function
func (f *Future[T]) Await() T {
	// TODO: configurable timeout
	res, err := f.f.Get(f.testEnv.Context(defaultWaiterPolling))
	if err != nil {
		panic(fmt.Sprintf("Future[T] Await() failed: %v", err))
	}
	return res
}

func (f *Future[T]) Cancel() {
	if f.cancel != nil {
		f.cancel()
	}
}
