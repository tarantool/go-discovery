package discovery_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
)

const (
	ttServer   = "127.0.0.1:3013"
	ttUsername = "testuser"
	ttPassword = "testpass"
)

var (
	dialer = tarantool.NetDialer{
		Address:  ttServer,
		User:     ttUsername,
		Password: ttPassword,
	}
	opts = tarantool.Opts{
		Timeout: 5 * time.Second,
	}
)

var startOpts test_helpers.StartOpts = test_helpers.StartOpts{
	Dialer:       dialer,
	InitScript:   "testdata/init.lua",
	Listen:       ttServer,
	WaitStart:    100 * time.Millisecond,
	ConnectRetry: 3,
	RetryTimeout: 500 * time.Millisecond,
}

func startTarantool(t testing.TB) *test_helpers.TarantoolInstance {
	t.Helper()

	inst, err := test_helpers.StartTarantool(startOpts)
	if err != nil {
		if inst != nil {
			stopTarantool(inst)
		}
		t.Fatalf("Failed to prepare Tarantool: %s", err)
	}
	return inst
}

func stopTarantool(instance *test_helpers.TarantoolInstance) {
	if instance == nil {
		return
	}
	test_helpers.StopTarantoolWithCleanup(instance)
}

func TestTarantoolWorks(t *testing.T) {
	defer stopTarantool(startTarantool(t))

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	_, err := conn.Do(tarantool.NewPingRequest()).Get()

	require.NoError(t, err)
}
