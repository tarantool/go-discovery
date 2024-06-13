package dial_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tarantool/go-tarantool/v2"

	"github.com/tarantool/go-discovery/dial"
)

var _ tarantool.Dialer = &dial.CompositeDialer{}

type connMock struct {
	tarantool.Conn
	Value int
}

type dialerMock struct {
	Calls int
	Ctx   context.Context
	Opts  tarantool.DialOpts
	Conn  tarantool.Conn
	Err   error
}

func (d *dialerMock) Dial(ctx context.Context,
	opts tarantool.DialOpts) (tarantool.Conn, error) {
	d.Calls++
	d.Ctx = ctx
	d.Opts = opts
	return d.Conn, d.Err
}

func TestCompositeDialer_Dial(t *testing.T) {
	conn1 := connMock{Value: 1}
	conn2 := connMock{Value: 2}
	ctx := context.Background()
	opts := tarantool.DialOpts{IoTimeout: time.Second}

	cases := []struct {
		Name    string
		Dialers []tarantool.Dialer
		Conn    tarantool.Conn
		Err     error
	}{
		{
			Name:    "dialers is nil",
			Dialers: nil,
			Conn:    nil,
			Err:     errors.New("dialers list is empty"),
		},
		{
			Name:    "dialers is empty",
			Dialers: []tarantool.Dialer{},
			Conn:    nil,
			Err:     errors.New("dialers list is empty"),
		},
		{
			Name: "dialers return errors",
			Dialers: []tarantool.Dialer{
				&dialerMock{
					Err: errors.New("first"),
				},
				&dialerMock{
					Err: errors.New("second"),
				},
			},
			Conn: nil,
			Err:  errors.Join(errors.New("first"), errors.New("second")),
		},
		{
			Name: "skip errors",
			Dialers: []tarantool.Dialer{
				&dialerMock{
					Err: errors.New("first"),
				},
				&dialerMock{
					Err: errors.New("second"),
				},
				&dialerMock{
					Conn: conn1,
				},
			},
			Conn: conn1,
			Err:  nil,
		},
		{
			Name: "returns first conn",
			Dialers: []tarantool.Dialer{
				&dialerMock{
					Conn: conn1,
				},
				&dialerMock{
					Conn: conn2,
				},
			},
			Conn: conn1,
			Err:  nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			dialer := dial.CompositeDialer{tc.Dialers}

			conn, err := dialer.Dial(ctx, opts)

			// We check a fact of a call of nested dialers here.
			noConn := true
			for _, dialer := range tc.Dialers {
				mock := dialer.(*dialerMock)
				if noConn {
					assert.Equal(t, 1, mock.Calls)
					assert.Equal(t, ctx, mock.Ctx)
					assert.Equal(t, opts, mock.Opts)
					if mock.Err == nil {
						noConn = false
					}
				} else {
					assert.Equal(t, 0, mock.Calls)
				}
			}

			// We check a result here.
			assert.Equal(t, tc.Conn, conn)
			assert.Equal(t, tc.Err, err)
		})
	}
}

func TestCompositeDialer_Dial_panic_on_nil_dialer(t *testing.T) {
	dialer := dial.CompositeDialer{[]tarantool.Dialer{nil}}

	// It is expected, just don't pass nil as a Dialer.
	assert.Panics(t, func() {
		_, err := dialer.Dial(context.Background(), tarantool.DialOpts{})
		// It makes the linter happy.
		assert.NoError(t, err)
	})
}

func TestCompositeDialer_Dial_expired_context(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	dialer := dial.CompositeDialer{[]tarantool.Dialer{nil}}
	conn, err := dialer.Dial(ctx, tarantool.DialOpts{})

	assert.Nil(t, conn)
	assert.Equal(t, context.Canceled, err)
}
