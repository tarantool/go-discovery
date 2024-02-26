package pool_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/pool"
)

var _ tarantool.Doer = &pool.DoerAdapter{}

type mockRequest struct {
	tarantool.Request
}

type mockModeDoer struct {
	Request tarantool.Request
	Mode    discovery.Mode
	Ret     *tarantool.Future
}

func (d *mockModeDoer) Do(request tarantool.Request,
	mode discovery.Mode) *tarantool.Future {
	d.Request = request
	d.Mode = mode
	return d.Ret
}

func TestModeDoer(t *testing.T) {
	modes := []discovery.Mode{discovery.ModeRO, discovery.ModeRW, discovery.ModeAny}
	request := mockRequest{}
	future := &tarantool.Future{}

	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			modeDoer := &mockModeDoer{
				Ret: future,
			}

			doer := pool.NewDoerAdapter(modeDoer, mode)
			require.NotNil(t, doer)

			fut := doer.Do(request)

			// We need to ensure that ModeAdapter just pass values as is.
			assert.Equal(t, request, modeDoer.Request)
			assert.Equal(t, mode, modeDoer.Mode)
			assert.Equal(t, future, fut)
		})
	}
}
