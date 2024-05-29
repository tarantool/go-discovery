package pool_test

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/test_helpers"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/dial"
	"github.com/tarantool/go-discovery/pool"

	"golang.org/x/exp/slices"
)

const (
	host       = "127.0.0.1"
	user       = "test"
	pass       = "test"
	initScript = "testdata/init.lua"
)

var ports = []string{"3013", "3014", "3015", "3016", "3017"}
var servers = []string{
	strings.Join([]string{host, ports[0]}, ":"),
	strings.Join([]string{host, ports[1]}, ":"),
	strings.Join([]string{host, ports[2]}, ":"),
	strings.Join([]string{host, ports[3]}, ":"),
	strings.Join([]string{host, ports[4]}, ":"),
}

func makeDialer(server string) tarantool.Dialer {
	return tarantool.NetDialer{
		Address:  server,
		User:     user,
		Password: pass,
	}
}

func makeDialers(servers []string) []tarantool.Dialer {
	dialers := make([]tarantool.Dialer, 0, len(servers))
	for _, server := range servers {
		dialers = append(dialers, makeDialer(server))
	}
	return dialers
}

func makeInstance(server string) discovery.Instance {
	return discovery.Instance{
		Name: server,
		URI:  []string{server},
	}
}

func makeInstances(servers []string) []discovery.Instance {
	instances := []discovery.Instance{}
	for _, server := range servers {
		instances = append(instances, makeInstance(server))
	}
	return instances
}

func makeAddEvents(servers []string) []discovery.Event {
	events := []discovery.Event{}
	for _, server := range servers {
		events = append(events, discovery.Event{
			Type: discovery.EventTypeAdd,
			New:  makeInstance(server),
		})
	}
	return events
}

func makeRemoveEvents(servers []string) []discovery.Event {
	events := []discovery.Event{}
	for _, server := range servers {
		events = append(events, discovery.Event{
			Type: discovery.EventTypeRemove,
			Old:  makeInstance(server),
		})
	}
	return events
}

var dialers = makeDialers(servers)

var connOpts = tarantool.Opts{
	Timeout: 5 * time.Second,
}

type holdInstancesMockBalancer struct {
	pool.Balancer
	mut       sync.Mutex
	instances []discovery.Instance
	changed   chan struct{}
}

func newHoldInstancesMockBalancer(base pool.Balancer) *holdInstancesMockBalancer {
	return &holdInstancesMockBalancer{
		Balancer:  base,
		instances: []discovery.Instance{},
		changed:   make(chan struct{}, 1024),
	}
}

func (b *holdInstancesMockBalancer) Add(instance discovery.Instance) error {
	b.mut.Lock()
	defer b.mut.Unlock()

	if err := b.Balancer.Add(instance); err != nil {
		return err
	}

	b.instances = append(b.instances, instance)
	b.changed <- struct{}{}
	return nil
}

func (b *holdInstancesMockBalancer) Remove(name string) {
	b.mut.Lock()
	defer b.mut.Unlock()

	b.instances = slices.DeleteFunc(b.instances, func(item discovery.Instance) bool {
		return item.Name == name
	})
	b.Balancer.Remove(name)
	b.changed <- struct{}{}
}

type retMockBalancer struct {
	Ret chan string
}

func newRetMockBalancer() *retMockBalancer {
	return &retMockBalancer{
		Ret: make(chan string, 1024),
	}
}

func (b *retMockBalancer) Add(_ discovery.Instance) error {
	return nil
}

func (b *retMockBalancer) Remove(_ string) {
}

func (b retMockBalancer) Next(_ discovery.Mode) (string, bool) {
	select {
	case value, ok := <-b.Ret:
		if ok {
			return value, true
		}
	default:
	}
	return "", false
}

func waitInstances(t testing.TB,
	balancer *holdInstancesMockBalancer, instances []discovery.Instance) {
	sortCmp := func(left, right discovery.Instance) int {
		return strings.Compare(left.Name, right.Name)
	}
	equalCmp := func(left, right discovery.Instance) int {
		if reflect.DeepEqual(left, right) {
			return 0
		}
		return -1
	}
	slices.SortFunc(instances, sortCmp)

	timeout := time.After(5 * time.Second)
	for {
		balancer.mut.Lock()
		balancerInstances := slices.Clone(balancer.instances)
		balancer.mut.Unlock()

		slices.SortFunc(balancerInstances, sortCmp)
		if slices.CompareFunc(instances, balancerInstances, equalCmp) == 0 {
			return
		}

		select {
		case <-timeout:
			require.Equal(t, instances, balancerInstances)
		case <-balancer.changed:
		}
	}
}

func startPool(t testing.TB) []test_helpers.TarantoolInstance {
	if err := assertTarantoolVersion(); err != nil {
		t.Fatalf(err.Error())
	}

	waitStart := 100 * time.Millisecond
	connectRetry := 10
	retryTimeout := 500 * time.Millisecond

	opts := make([]test_helpers.StartOpts, 0, len(servers))
	for _, serv := range servers {
		opts = append(opts, test_helpers.StartOpts{
			Listen: serv,
			Dialer: tarantool.NetDialer{
				Address:  serv,
				User:     user,
				Password: pass,
			},
			InitScript:   initScript,
			WaitStart:    waitStart,
			ConnectRetry: connectRetry,
			RetryTimeout: retryTimeout,
		})
	}

	instances, err := test_helpers.StartTarantoolInstances(opts)
	require.NoError(t, err)
	return instances
}

func assertTarantoolVersion() error {
	tooOld, err := test_helpers.IsTarantoolVersionLess(3, 0, 0)
	if err != nil {
		return fmt.Errorf("Could not check the Tarantool version: %w", err)
	}

	if tooOld {
		return fmt.Errorf("Tarantool 3 is required (library uses WATCH_ONCE)")
	}

	return nil
}

func stopPool(_ testing.TB, instances []test_helpers.TarantoolInstance) {
	test_helpers.StopTarantoolInstances(instances)
}

func TestPool_simple(t *testing.T) {
	defer stopPool(t, startPool(t))
	roles := []bool{false, true, false, false, true}
	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.NoError(t, err)

	factory := dial.NewNetDialerFactory(user, pass, connOpts)
	balancer := pool.NewRoundRobinBalancer()
	testPool, err := pool.NewPool(factory, balancer)
	require.NotNil(t, testPool)
	require.NoError(t, err)

	testPool.Observe(makeAddEvents(servers), nil)
	defer testPool.Observe(nil, discovery.ErrUnsubscribe)

	modes := []discovery.Mode{discovery.ModeAny, discovery.ModeRO, discovery.ModeRW}
	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			timeout := time.After(5 * time.Second)
			for {
				select {
				case <-timeout:
					t.Fatalf("timeout")
				default:
				}

				req := tarantool.NewPingRequest()
				_, err := testPool.Do(req, mode).Get()
				if errors.Is(err, pool.ErrNoConnectedInstances) {
					continue
				}
				require.Nil(t, err)
				return
			}
		})
	}
}

func TestPool_Observe(t *testing.T) {
	defer stopPool(t, startPool(t))
	roles := []bool{false, true, false, false, true}
	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.NoError(t, err)

	factory := dial.NewNetDialerFactory(user, pass, connOpts)
	balancer := newHoldInstancesMockBalancer(newRetMockBalancer())
	testPool, err := pool.NewPool(factory, balancer)
	require.NotNil(t, testPool)
	require.NoError(t, err)

	// Start with all instances.
	instances := makeInstances(servers)
	// The pool determines an instance mode, so we need to set it.
	require.Equal(t, len(instances), len(roles))
	for i, role := range roles {
		switch role {
		case true:
			instances[i].Mode = discovery.ModeRO
		case false:
			instances[i].Mode = discovery.ModeRW
		}
	}

	// It could be broken down into more test cases, but the pool starts too
	// slowly, so let's just do it all here in a sequence.

	testPool.Observe(makeAddEvents(servers), nil)
	defer testPool.Observe(nil, discovery.ErrUnsubscribe)
	waitInstances(t, balancer, instances)

	// Remove some.
	testPool.Observe(makeRemoveEvents(servers[:3]), nil)
	waitInstances(t, balancer, instances[3:])

	// Add it again.
	testPool.Observe(makeAddEvents(servers[:3]), nil)
	waitInstances(t, balancer, instances)

	// Update.
	updateInstance := instances[0]
	updateInstance.URI = instances[1].URI
	updateInstance.Mode = instances[1].Mode

	testPool.Observe([]discovery.Event{
		discovery.Event{
			Type: discovery.EventTypeUpdate,
			Old:  instances[0],
			New:  updateInstance,
		},
	}, nil)
	waitInstances(t, balancer, append(instances[1:], updateInstance))

	// Done with some error.
	testPool.Observe(nil, fmt.Errorf("any"))
	waitInstances(t, balancer, []discovery.Instance{})
}

func TestPool_stop_and_start_instances(t *testing.T) {
	ttInstances := startPool(t)
	defer stopPool(t, ttInstances)

	roles := []bool{false, true, false, false, true}
	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.NoError(t, err)

	factory := dial.NewNetDialerFactory(user, pass, connOpts)
	balancer := newHoldInstancesMockBalancer(newRetMockBalancer())
	testPool, err := pool.NewPool(factory, balancer)
	require.NotNil(t, testPool)
	require.NoError(t, err)

	// Start with all instances.
	instances := makeInstances(servers)
	// The pool determines an instance mode, so we need to set it.
	require.Equal(t, len(instances), len(roles))
	for i, role := range roles {
		switch role {
		case true:
			instances[i].Mode = discovery.ModeRO
		case false:
			instances[i].Mode = discovery.ModeRW
		}
	}

	testPool.Observe(makeAddEvents(servers), nil)
	defer testPool.Observe(nil, discovery.ErrUnsubscribe)
	waitInstances(t, balancer, instances)

	// Stop some.
	for _, ttInstance := range ttInstances[:3] {
		test_helpers.StopTarantoolWithCleanup(ttInstance)
	}
	waitInstances(t, balancer, instances[3:])

	// And start it again.
	for i := range ttInstances[:3] {
		err := test_helpers.RestartTarantool(&ttInstances[i])
		require.NoError(t, err)
	}
	// Don't forget to restore roles, here we also ensure that the pool
	// could update an instance role.
	err = test_helpers.SetClusterRO(dialers[:3], connOpts, roles[:3])
	require.NoError(t, err)

	waitInstances(t, balancer, instances)
}

func TestPool_choose_instance_by_a_balancer(t *testing.T) {
	defer stopPool(t, startPool(t))

	roles := []bool{false, true, false, false, true}
	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.NoError(t, err)

	factory := dial.NewNetDialerFactory(user, pass, connOpts)
	retBalancer := newRetMockBalancer()
	balancer := newHoldInstancesMockBalancer(retBalancer)
	testPool, err := pool.NewPool(factory, balancer)
	require.NotNil(t, testPool)
	require.NoError(t, err)

	// Start with all instances.
	instances := makeInstances(servers)
	// The pool determines an instance mode, so we need to set it.
	require.Equal(t, len(instances), len(roles))
	for i, role := range roles {
		switch role {
		case true:
			instances[i].Mode = discovery.ModeRO
		case false:
			instances[i].Mode = discovery.ModeRW
		}
	}

	testPool.Observe(makeAddEvents(servers), nil)
	defer testPool.Observe(nil, discovery.ErrUnsubscribe)
	waitInstances(t, balancer, instances)

	ping := tarantool.NewPingRequest()
	eval := tarantool.NewEvalRequest("return box.cfg.listen")
	modes := []discovery.Mode{discovery.ModeAny, discovery.ModeRO, discovery.ModeRW}

	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			// Balancer returns no instance.
			_, err = testPool.Do(ping, mode).Get()
			assert.Equal(t, pool.ErrNoConnectedInstances, err)

			// Balancer returns an unknown instance and then no instance.
			// The unknown instance just skipped.
			retBalancer.Ret <- "some unknown"
			_, err = testPool.Do(ping, mode).Get()
			assert.Equal(t, pool.ErrNoConnectedInstances, err)

			// Execute a request on each instance, the pool ignores instance
			// modes and just does what the Balancer says.
			for _, server := range servers {
				timeout := time.After(5 * time.Second)
				for {
					select {
					case <-timeout:
						t.Fatalf("timeout")
					default:
					}

					retBalancer.Ret <- server
					data, err := testPool.Do(eval, mode).Get()
					if errors.Is(err, pool.ErrNoConnectedInstances) {
						continue
					}
					require.NoError(t, err)
					assert.Equal(t, []interface{}{server}, data)
					break
				}
			}
		})
	}
}

func TestPool_balancers_concurrent(t *testing.T) {
	defer stopPool(t, startPool(t))
	roles := []bool{false, true, false, false, true}
	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.NoError(t, err)

	instances := makeInstances(servers)
	require.Equal(t, len(instances), len(roles))
	for i, role := range roles {
		switch role {
		case true:
			instances[i].Mode = discovery.ModeRO
		case false:
			instances[i].Mode = discovery.ModeRW
		}
	}

	cases := []struct {
		Name     string
		Balancer pool.Balancer
	}{
		{"round_robin", pool.NewRoundRobinBalancer()},
		{"priority", pool.NewPriorityBalancer(func(_ discovery.Instance) int {
			return 0
		})},
	}
	factory := dial.NewNetDialerFactory(user, pass, connOpts)

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			balancer := newHoldInstancesMockBalancer(tc.Balancer)
			testPool, err := pool.NewPool(factory, balancer)
			require.NotNil(t, testPool)
			require.NoError(t, err)

			var wg sync.WaitGroup
			done := make(chan struct{})
			for i := 0; i < 1000; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					select {
					case <-done:
						return
					default:
					}

					// We aren't interested in a result, just ensure that
					// nothing panics and hangs.
					fut := testPool.Do(tarantool.NewPingRequest(), discovery.ModeAny)
					assert.NotNil(t, fut)
				}()
			}

			testPool.Observe(makeAddEvents(servers), nil)
			defer testPool.Observe(nil, discovery.ErrUnsubscribe)

			waitInstances(t, balancer, instances)

			testPool.Observe(nil, discovery.ErrUnsubscribe)
			wg.Wait()
		})
	}
}
