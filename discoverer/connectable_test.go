package discoverer_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/dial"
	"github.com/tarantool/go-discovery/discoverer"
)

var _ discovery.Discoverer = discoverer.NewConnectable(nil, nil)

type mockIoConn struct {
	// Sends an event on Read()/Write().
	read, written chan struct{}
	// Read()/Write() buffers.
	readbuf, writebuf bytes.Buffer
	// Close buffer.
	closed chan struct{}
	// Context to stop conn.
	ctx context.Context
}

func (m *mockIoConn) Read(b []byte) (int, error) {
	<-m.written
	m.written <- struct{}{}

	ret, err := m.readbuf.Read(b)

	if ret != 0 && m.read != nil {
		select {
		case m.read <- struct{}{}:
			break
		case <-m.ctx.Done():
			return 0, m.ctx.Err()
		}
	}

	return ret, err
}

func (m *mockIoConn) Write(b []byte) (int, error) {
	ret, err := m.writebuf.Write(b)

	if m.written != nil {
		m.written <- struct{}{}
	}

	return ret, err
}

func (m *mockIoConn) Flush() error {
	return nil
}

func (m *mockIoConn) Close() error {
	m.closed <- struct{}{}
	return nil
}

func (m *mockIoConn) Greeting() tarantool.Greeting {
	return tarantool.Greeting{}
}

func (m *mockIoConn) ProtocolInfo() tarantool.ProtocolInfo {
	return tarantool.ProtocolInfo{}
}

func (m *mockIoConn) Addr() net.Addr {
	return nil
}

type mockIoDialer struct {
	init func(conn *mockIoConn)
	conn *mockIoConn
	dial chan struct{}
}

func newMockIoConn(ctx context.Context) *mockIoConn {
	conn := new(mockIoConn)
	conn.closed = make(chan struct{}, 1)
	conn.written = make(chan struct{}, 1)
	conn.ctx = ctx
	return conn
}

func (m *mockIoDialer) Dial(ctx context.Context,
	_ tarantool.DialOpts) (tarantool.Conn, error) {

	m.conn = newMockIoConn(ctx)
	if m.init != nil {
		m.init(m.conn)
	}

	if m.dial != nil {
		m.dial <- struct{}{}
	}
	return m.conn, nil
}

type mockDialerFactory struct {
	dialer map[string]*mockIoDialer
	opts   tarantool.Opts
	err    map[string]error
}

func (f *mockDialerFactory) NewDialer(instance discovery.Instance) (tarantool.Dialer,
	tarantool.Opts, error) {

	if err, ok := f.err[instance.Name]; ok {
		return nil, f.opts, err
	}
	return f.dialer[instance.Name], f.opts, nil
}

func TestConnectable_Discovery_nilFactory(t *testing.T) {
	disc := discoverer.NewConnectable(nil, nil)
	assert.NotNil(t, disc)

	instances, err := disc.Discovery(context.Background())
	assert.Nil(t, instances)
	assert.Equal(t, discoverer.ErrMissingFactory, err)
}

func TestConnectable_Discovery_nilDiscoverer(t *testing.T) {
	disc := discoverer.NewConnectable(&dial.NetDialerFactory{}, nil)
	assert.NotNil(t, disc)

	instances, err := disc.Discovery(context.Background())
	assert.Nil(t, instances)
	assert.Equal(t, discoverer.ErrMissingDiscoverer, err)
}

func TestConnectable_Discovery_contextCanceled(t *testing.T) {
	disc := discoverer.NewConnectable(&dial.NetDialerFactory{},
		&mockDiscoverer{})
	assert.NotNil(t, disc)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	instances, err := disc.Discovery(ctx)
	assert.Nil(t, instances)
	assert.Equal(t, context.Canceled, err)
}

var (
	roInd = 46

	runningRWResponse = []byte{
		0xce, 0x00, 0x00, 0x00, 68, // Length.
		0x83, // Header map.
		0x00, 0xce,
		0x00, 0x00, 0x00, 0x00,
		0x01, 0xcf,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		3, // Request ID.
		0x05, 0xcf, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x55, 0x81, 0x30, 0xdd, 0x00, 0x00, 0x00, 0x01,

		0x83,
		0xa5, 'i', 's', '_', 'r', 'o',
		0xc2,
		0xa9, 'i', 's', '_', 'r', 'o', '_', 'c', 'f', 'g',
		0xc2,
		0xa6, 's', 't', 'a', 't', 'u', 's',
		0xa7, 'r', 'u', 'n', 'n', 'i', 'n', 'g',
	}
)

func TestConnectable_Discovery(t *testing.T) {
	cases := []struct {
		name      string
		instances []discovery.Instance
		result    []discovery.Instance
	}{
		{
			name: "single instance",
			instances: []discovery.Instance{
				{
					Name: "single",
				},
			},
			result: []discovery.Instance{
				{
					Name: "single",
					Mode: discovery.ModeRW,
				},
			},
		},
		{
			name: "single instance RO",
			instances: []discovery.Instance{
				{
					Name: "single",
					Mode: discovery.ModeRO,
				},
			},
			result: []discovery.Instance{
				{
					Name: "single",
					Mode: discovery.ModeRO,
				},
			},
		},
		{
			name: "two instances",
			instances: []discovery.Instance{
				{
					Name: "Man with no name",
				},
				{
					Name: "Col. Douglas Mortimer",
					Mode: discovery.ModeRO,
				},
			},
			result: []discovery.Instance{
				{
					Name: "Man with no name",
					Mode: discovery.ModeRW,
				},
				{
					Name: "Col. Douglas Mortimer",
					Mode: discovery.ModeRO,
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			factory := mockDialerFactory{
				dialer: make(map[string]*mockIoDialer),
				opts: tarantool.Opts{
					SkipSchema: true,
					Timeout:    1000 * time.Second,
				},
				err: make(map[string]error),
			}

			for _, inst := range tc.instances {
				inst := inst
				dialer := mockIoDialer{
					init: func(conn *mockIoConn) {
						response := make([]byte, len(runningRWResponse))
						copy(response, runningRWResponse)

						if inst.Mode == discovery.ModeRO {
							response[roInd] = 0xc3
						}

						conn.readbuf.Write(response)
					},
				}

				factory.dialer[inst.Name] = &dialer
			}

			innerDisk := mockDiscoverer{
				instances: tc.instances,
			}

			disc := discoverer.NewConnectable(&factory, &innerDisk)
			assert.NotNil(t, disc)

			inst, err := disc.Discovery(context.Background())
			assert.NoError(t, err)
			assert.Equal(t, len(tc.result), len(inst))
			for _, instance := range inst {
				assert.Contains(t, tc.result, instance)
			}
		})
	}
}

func TestConnectable_Discovery_factoryError(t *testing.T) {
	dialer := mockIoDialer{
		init: func(conn *mockIoConn) {
			conn.readbuf.Write(runningRWResponse)
		},
	}

	factory := mockDialerFactory{
		dialer: make(map[string]*mockIoDialer),
		opts: tarantool.Opts{
			SkipSchema: true,
			Timeout:    1000 * time.Second,
		},
		err: make(map[string]error),
	}
	factory.dialer["the good"] = &dialer
	factory.err["the bad"] = fmt.Errorf("the ugly")

	innerDisk := mockDiscoverer{
		instances: []discovery.Instance{
			{
				Name: "the good",
			},
			{
				Name: "the bad",
			},
		},
	}

	disc := discoverer.NewConnectable(&factory, &innerDisk)
	assert.NotNil(t, disc)

	inst, err := disc.Discovery(context.Background())
	assert.NoError(t, err)
	require.Equal(t, 1, len(inst))
	assert.Equal(t, discovery.Instance{
		Name: "the good",
		Mode: discovery.ModeRW,
	}, inst[0])
}

func TestConnectable_Discovery_contextCancel_inProgress(t *testing.T) {
	factory := mockDialerFactory{
		dialer: make(map[string]*mockIoDialer),
		opts: tarantool.Opts{
			SkipSchema: true,
			Timeout:    1000 * time.Second,
		},
		err: make(map[string]error),
	}

	innerDisk := mockDiscoverer{
		instances: []discovery.Instance{
			{
				Name: "Blondie",
			},
			{
				Name: "Ramon Rojo",
			},
		},
	}

	factory.dialer["Blondie"] = &mockIoDialer{
		init: func(conn *mockIoConn) {
			conn.readbuf.Write(runningRWResponse)
		},
		dial: make(chan struct{}, 1),
	}
	factory.dialer["Ramon Rojo"] = &mockIoDialer{
		init: func(conn *mockIoConn) {
			conn.readbuf.Write(runningRWResponse)
			conn.read = make(chan struct{}, 1)
			conn.read <- struct{}{}
		},
		dial: make(chan struct{}, 1),
	}

	disc := discoverer.NewConnectable(&factory, &innerDisk)
	assert.NotNil(t, disc)

	ctx, cancel := context.WithCancel(context.Background())

	var (
		inst []discovery.Instance
		err  error
	)

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		inst, err = disc.Discovery(ctx)
		wg.Done()
	}()

	// Wait for the creation of connections.
	<-factory.dialer["Blondie"].dial
	<-factory.dialer["Ramon Rojo"].dial
	// Wait for a successful read.
	<-factory.dialer["Blondie"].conn.closed
	// Cancel connection to the second instance.
	cancel()
	wg.Wait()

	// Check that second connection is also closed.
	select {
	case <-factory.dialer["Ramon Rojo"].conn.closed:
		break
	default:
		t.Errorf("One of the connections is still alive")
	}

	// We got an error and an instance, that we managed to find as connectable.
	assert.Equal(t, context.Canceled, err)
	assert.Equal(t, []discovery.Instance{
		{
			Name: "Blondie",
			Mode: discovery.ModeRW,
		},
	}, inst)
}
