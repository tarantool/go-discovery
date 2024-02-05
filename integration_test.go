package discovery_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

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

func startTarantool(t testing.TB) test_helpers.TarantoolInstance {
	t.Helper()

	inst, err := test_helpers.StartTarantool(startOpts)
	if err != nil {
		stopTarantool(inst)
		t.Fatalf("Failed to prepare Tarantool: %s", err)
	}
	return inst
}

func stopTarantool(instance test_helpers.TarantoolInstance) {
	test_helpers.StopTarantoolWithCleanup(instance)
}

const (
	etcdBaseEndpoint  = "127.0.0.1:12379"
	etcdHTTPEndpoint  = "http://" + etcdBaseEndpoint
	etcdHTTPSEndpoint = "https://" + etcdBaseEndpoint
	etcdTimeout       = 5 * time.Second
)

type etcdOpts struct {
	Username string
	Password string
	KeyFile  string
	CertFile string
	CaFile   string
}

type etcdInstance struct {
	Cmd *exec.Cmd
	Dir string
}

func startEtcd(t *testing.T, endpoint string, opts etcdOpts) etcdInstance {
	t.Helper()

	mydir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %s", err)
	}

	inst := etcdInstance{}
	dir, err := os.MkdirTemp("", "work_dir")
	if err != nil {
		t.Fatalf("Failed to create a temporary directory: %s", err)
	}
	inst.Dir = dir
	inst.Cmd = exec.Command("etcd")

	inst.Cmd.Env = append(
		os.Environ(),
		fmt.Sprintf("ETCD_LISTEN_CLIENT_URLS=%s", endpoint),
		fmt.Sprintf("ETCD_ADVERTISE_CLIENT_URLS=%s", endpoint),
		fmt.Sprintf("ETCD_DATA_DIR=%s", inst.Dir),
	)
	if opts.KeyFile != "" {
		keyPath := filepath.Join(mydir, opts.KeyFile)
		inst.Cmd.Env = append(inst.Cmd.Env,
			fmt.Sprintf("ETCD_KEY_FILE=%s", keyPath))
	}
	if opts.CertFile != "" {
		certPath := filepath.Join(mydir, opts.CertFile)
		inst.Cmd.Env = append(inst.Cmd.Env,
			fmt.Sprintf("ETCD_CERT_FILE=%s", certPath))
	}
	if opts.CaFile != "" {
		caPath := filepath.Join(mydir, opts.CaFile)
		inst.Cmd.Env = append(inst.Cmd.Env,
			fmt.Sprintf("ETCD_TRUSTED_CA_FILE=%s", caPath))
	}

	// Start etcd.
	err = inst.Cmd.Start()
	if err != nil {
		os.RemoveAll(inst.Dir)
		t.Fatalf("Failed to start etcd: %s", err)
	}

	// Setup user/pass.
	if opts.Username != "" {
		cmd := exec.Command("etcdctl", "user", "add", opts.Username,
			fmt.Sprintf("--new-user-password=%s", opts.Password),
			fmt.Sprintf("--endpoints=%s", etcdBaseEndpoint))

		err := cmd.Run()
		if err != nil {
			stopEtcd(t, inst)
			t.Fatalf("Failed to create user: %s", err)
		}

		if opts.Username != "root" {
			// We need the root user for auth enable anyway.
			cmd := exec.Command("etcdctl", "user", "add", "root",
				fmt.Sprintf("--new-user-password=%s", opts.Password),
				fmt.Sprintf("--endpoints=%s", etcdBaseEndpoint))

			err := cmd.Run()
			if err != nil {
				stopEtcd(t, inst)
				t.Fatalf("Failed to create root: %s", err)
			}

			// And additional permissions for a regular user.
			cmd = exec.Command("etcdctl", "user", "grant-role", opts.Username,
				"root", fmt.Sprintf("--endpoints=%s", etcdBaseEndpoint))

			err = cmd.Run()
			if err != nil {
				stopEtcd(t, inst)
				t.Fatalf("Failed to grant-role: %s", err)
			}
		}

		cmd = exec.Command("etcdctl", "auth", "enable",
			fmt.Sprintf("--user=root:%s", opts.Password),
			fmt.Sprintf("--endpoints=%s", etcdBaseEndpoint))

		err = cmd.Run()
		if err != nil {
			stopEtcd(t, inst)
			t.Fatalf("Failed to enable auth: %s", err)
		}
	}

	return inst
}

func stopEtcd(t *testing.T, inst etcdInstance) {
	t.Helper()

	if inst.Cmd != nil && inst.Cmd.Process != nil {
		if err := inst.Cmd.Process.Kill(); err != nil {
			t.Fatalf("Failed to kill etcd (%d) %s", inst.Cmd.Process.Pid, err)
		}

		// Wait releases any resources associated with the Process.
		if _, err := inst.Cmd.Process.Wait(); err != nil {
			t.Fatalf("Failed to wait for etcd process to exit, got %s", err)
			return
		}

		inst.Cmd.Process = nil
	}

	if inst.Dir != "" {
		if err := os.RemoveAll(inst.Dir); err != nil {
			t.Fatalf("Failed to clean work directory, got %s", err)
		}
	}
}

func etcdPut(t *testing.T, etcd *clientv3.Client, key, value string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), etcdTimeout)
	presp, err := etcd.Put(ctx, key, value)
	cancel()
	require.NoError(t, err)
	require.NotNil(t, presp)
}

func etcdGet(t *testing.T, etcd *clientv3.Client, key string) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), etcdTimeout)
	resp, err := etcd.Get(ctx, key)
	cancel()

	require.NoError(t, err)
	require.NotNil(t, resp)
	if len(resp.Kvs) == 0 {
		return ""
	}

	require.Len(t, resp.Kvs, 1)
	return string(resp.Kvs[0].Value)
}

func TestTarantoolWorks(t *testing.T) {
	defer stopTarantool(startTarantool(t))

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	_, err := conn.Do(tarantool.NewPingRequest()).Get()

	require.NoError(t, err)
}

func TestEtcdWorks(t *testing.T) {
	defer stopEtcd(t, startEtcd(t, etcdHTTPEndpoint, etcdOpts{}))

	etcd, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdHTTPEndpoint},
		DialTimeout: etcdTimeout,
	})
	require.NoError(t, err)
	require.NotNil(t, etcd)
	defer etcd.Close()

	key := "foo"
	value := "bar"
	etcdPut(t, etcd, key, value)
	data := etcdGet(t, etcd, key)

	require.Equal(t, value, data)
}
