package dial

import (
	"context"
	"errors"

	"github.com/tarantool/go-tarantool/v2"
)

// CompositeDialer combines multiple dialers into one object.
type CompositeDialer struct {
	Dialers []tarantool.Dialer
}

// Dial tries to dial one by one dialer until success. It returns a merged
// error if unable to dial with any dialer.
func (d CompositeDialer) Dial(ctx context.Context,
	opts tarantool.DialOpts) (tarantool.Conn, error) {
	if len(d.Dialers) == 0 {
		return nil, errors.New("dialers list is empty")
	}

	var errs []error
	for _, dialer := range d.Dialers {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			conn, err := dialer.Dial(ctx, opts)
			if err == nil {
				return conn, nil
			}
			errs = append(errs, err)
		}
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return nil, errors.Join(errs...)
	}
}
