package subscriber_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tarantool/go-iproto"
	"github.com/tarantool/go-tarantool/v2"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/dial"
	"github.com/tarantool/go-discovery/subscriber"
)

type mockIoConn struct {
	// Sends an event on Read()/Write().
	read, written chan struct{}
	// Read()/Write() buffers.
	readbuf, writebuf bytes.Buffer
	// Close buffer.
	closed chan struct{}
	// Context to stop conn.
	ctx context.Context
	// Counter for WatchOnce request IDs.
	requestCnt int
	// Shows that enough requests were complete.
	requestsDone chan struct{}
}

func (m *mockIoConn) Read(b []byte) (int, error) {
	if m.written != nil {
		<-m.written
	}

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

	resp := make([]byte, len(runningRWResponse))
	copy(resp, runningRWResponse)
	resp[21] = byte(m.requestCnt)
	m.requestCnt += 2

	if m.requestCnt == 10 {
		m.requestsDone <- struct{}{}
	}

	m.readbuf.Write(resp)

	if m.written != nil {
		m.written <- struct{}{}
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
	return tarantool.ProtocolInfo{
		Features: []iproto.Feature{
			iproto.IPROTO_FEATURE_WATCH_ONCE,
		},
	}
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
	conn.requestCnt = 4
	conn.requestsDone = make(chan struct{}, 1)
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

func TestNewConnectable_NilFactory(t *testing.T) {
	connectable := subscriber.NewConnectable(nil, nil)
	assert.NotNil(t, connectable)

	obs := newMockObserver()
	err := connectable.Subscribe(context.Background(), obs)
	assert.Equal(t, subscriber.ErrMissingFactory, err)
}

func TestNewConnectable_NilSubscriber(t *testing.T) {
	connectable := subscriber.NewConnectable(&dial.NetDialerFactory{}, nil)
	assert.NotNil(t, connectable)

	obs := newMockObserver()
	err := connectable.Subscribe(context.Background(), obs)
	assert.Equal(t, subscriber.ErrMissingSubscriber, err)
}

func TestConnectable_Subscribe_NilObserver(t *testing.T) {
	sub := newMockSubscriber()

	connectable := subscriber.NewConnectable(&dial.NetDialerFactory{}, sub)
	assert.NotNil(t, connectable)

	err := connectable.Subscribe(context.Background(), nil)
	assert.Equal(t, discovery.ErrMissingObserver, err)
	assert.Equal(t, int32(0), sub.subCnt.Load())
	assert.Equal(t, int32(0), sub.unsubCnt.Load())
}

func TestConnectable_Subscribe_Concurrent(t *testing.T) {
	for i := 0; i < 100000; i++ {
		go func() {
			sub := newMockSubscriber()

			connectable := subscriber.NewConnectable(&dial.NetDialerFactory{}, sub)
			assert.NotNil(t, connectable)

			obs := newMockObserver()
			defer connectable.Unsubscribe(obs)

			wg := sync.WaitGroup{}
			wg.Add(2)
			var err1, err2 error

			go func() {
				err1 = connectable.Subscribe(context.Background(), obs)
				wg.Done()
			}()

			go func() {
				err2 = connectable.Subscribe(context.Background(), obs)
				wg.Done()
			}()
			wg.Wait()

			assert.True(t, (err1 == nil) != (err2 == nil),
				"Only one of subscriptions should succeed")
			assert.True(t, errors.Is(err1, subscriber.ErrSubscriptionExists) !=
				errors.Is(err2, subscriber.ErrSubscriptionExists),
				"Only one of subscriptions should fail with specific error")
			assert.Equal(t, int32(1), sub.subCnt.Load())
		}()
	}
}

var (
	runningRWResponse = []byte{
		0xce, 0x00, 0x00, 0x00, 68, // Length.
		0x83, // Header map.
		0x00, 0xce,
		0x00, 0x00, 0x00, 0x00,
		0x01, 0xcf,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		2, // Request ID.
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

func TestConnectable_Subscribe(t *testing.T) {
	cases := []struct {
		name      string
		instances []string
		events    []discovery.Event
		result    []discovery.Event
	}{
		{
			name:      "single instance",
			instances: []string{"Marv"},
			events: []discovery.Event{
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name: "Marv",
					},
				},
			},
			result: []discovery.Event{
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name: "Marv",
						Mode: discovery.ModeRW,
					},
				},
			},
		},
		{
			name:      "two instances",
			instances: []string{"Marv", "Hartigan"},
			events: []discovery.Event{
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name: "Marv",
					},
				},
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name: "Hartigan",
					},
				},
			},
			result: []discovery.Event{
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name: "Marv",
						Mode: discovery.ModeRW,
					},
				},
				{
					Type: discovery.EventTypeAdd,
					New: discovery.Instance{
						Name: "Hartigan",
						Mode: discovery.ModeRW,
					},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			events := len(tc.events)

			sub := newMockSubscriber()
			sub.eventsReturn = tc.events

			factory := mockDialerFactory{
				dialer: make(map[string]*mockIoDialer),
				opts: tarantool.Opts{
					SkipSchema: true,
					Timeout:    1000 * time.Second,
				},
				err: make(map[string]error),
			}

			for _, instance := range tc.instances {
				dialer := mockIoDialer{
					init: func(conn *mockIoConn) {
						conn.readbuf.Write(runningRWResponse)
						conn.written = make(chan struct{}, 2)
					},
					dial: make(chan struct{}, 1),
				}

				factory.dialer[instance] = &dialer
			}

			connectable := subscriber.NewConnectable(&factory, sub)
			assert.NotNil(t, connectable)

			obs := newMockObserver()
			obs.needCountEvents = true
			obs.eventWg.Add(events - 1)
			err := connectable.Subscribe(context.Background(), obs)
			assert.NoError(t, err)

			for _, instance := range tc.instances {
				<-factory.dialer[instance].dial
				<-factory.dialer[instance].conn.requestsDone
			}

			obs.eventWg.Wait()
			recentEvents := *obs.recentEvents.Load()
			assert.Equal(t, 1, len(recentEvents))
			assert.Contains(t, tc.result, recentEvents[0])

			obs.eventWg.Add(2)
			connectable.Unsubscribe(obs)

			assert.Equal(t, int32(1), obs.errCnt.Load())
			assert.Equal(t, discovery.ErrUnsubscribe, obs.recentErr)
		})
	}
}

func TestConnectable_Subscribe_factoryError(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	factory := mockDialerFactory{
		dialer: make(map[string]*mockIoDialer),
		opts: tarantool.Opts{
			SkipSchema: true,
			Timeout:    1000 * time.Second,
		},
		err: make(map[string]error),
	}
	factory.err["Roark"] = fmt.Errorf("some error")

	sub := newMockSubscriber()
	sub.eventsReturn = []discovery.Event{
		{
			Type: discovery.EventTypeAdd,
			New: discovery.Instance{
				Name: "Roark",
			},
		},
	}

	connectable := subscriber.NewConnectable(&factory, sub)
	assert.NotNil(t, connectable)

	obs := newMockObserver()
	err := connectable.Subscribe(context.Background(), obs)
	assert.NoError(t, err)

	assert.Equal(t, int32(0), obs.eventCnt.Load())

	connectable.Unsubscribe(obs)

	assert.Equal(t, int32(1), obs.errCnt.Load())
	assert.Equal(t, discovery.ErrUnsubscribe, obs.recentErr)

	assert.True(t, strings.Contains(buf.String(),
		"failed to create an instance dialer \"Roark\": some error"))
}

func TestConnectable_Subscribe_contextCancel_inProgress(t *testing.T) {
	factory := mockDialerFactory{
		dialer: make(map[string]*mockIoDialer),
		opts: tarantool.Opts{
			SkipSchema: true,
			Timeout:    1000 * time.Second,
		},
		err: make(map[string]error),
	}

	factory.dialer["Kevin"] = &mockIoDialer{
		init: func(conn *mockIoConn) {
			conn.readbuf.Write(runningRWResponse)
			conn.written = make(chan struct{}, 2)
			conn.read = make(chan struct{}, 1)
			conn.read <- struct{}{}
		},
		dial: make(chan struct{}, 1),
	}

	sub := newMockSubscriber()
	sub.eventsReturn = []discovery.Event{
		{
			Type: discovery.EventTypeAdd,
			New: discovery.Instance{
				Name: "Kevin",
			},
		},
	}

	connectable := subscriber.NewConnectable(&factory, sub)
	assert.NotNil(t, connectable)

	obs := newMockObserver()
	ctx, cancel := context.WithCancel(context.Background())

	err := connectable.Subscribe(ctx, obs)
	assert.NoError(t, err)

	<-factory.dialer["Kevin"].dial

	cancel()

	assert.Equal(t, int32(0), obs.eventCnt.Load())
	assert.Equal(t, []discovery.Event{}, *obs.recentEvents.Load())

	assert.Equal(t, int32(0), obs.errCnt.Load())
	assert.Equal(t, nil, obs.recentErr)
}

func TestConnectable_Subscribe_Update(t *testing.T) {
	factory := mockDialerFactory{
		dialer: make(map[string]*mockIoDialer),
		opts: tarantool.Opts{
			SkipSchema: true,
			Timeout:    1000 * time.Second,
		},
		err: make(map[string]error),
	}

	factory.dialer["McCarthy"] = &mockIoDialer{
		init: func(conn *mockIoConn) {
			conn.readbuf.Write(runningRWResponse)
			conn.written = make(chan struct{}, 2)
		},
		dial: make(chan struct{}, 1),
	}

	sub := newMockSubscriber()
	sub.eventsReturn = []discovery.Event{
		{
			Type: discovery.EventTypeAdd,
			New: discovery.Instance{
				Name:  "McCarthy",
				Group: "Old",
			},
		},
	}

	connectable := subscriber.NewConnectable(&factory, sub)
	assert.NotNil(t, connectable)

	obs := newMockObserver()
	obs.needCountEvents = true
	err := connectable.Subscribe(context.Background(), obs)
	assert.NoError(t, err)

	<-factory.dialer["McCarthy"].dial
	<-factory.dialer["McCarthy"].conn.requestsDone

	obs.eventWg.Wait()
	obs.eventWg.Add(2)

	assert.Equal(t, int32(1), obs.eventCnt.Load())
	assert.Equal(t, []discovery.Event{
		{
			Type: discovery.EventTypeAdd,
			New: discovery.Instance{
				Name:  "McCarthy",
				Group: "Old",
				Mode:  discovery.ModeRW,
			},
		},
	}, *obs.recentEvents.Load())

	sub.eventsReturn = []discovery.Event{
		{
			Type: discovery.EventTypeUpdate,
			Old: discovery.Instance{
				Name:  "McCarthy",
				Group: "Old",
			},
			New: discovery.Instance{
				Name:  "McCarthy",
				Group: "New",
			},
		},
	}
	<-sub.eventWait

	<-factory.dialer["McCarthy"].dial
	<-factory.dialer["McCarthy"].conn.requestsDone

	obs.eventWg.Wait()

	// Add old.
	// Update: remove old, add new.
	assert.True(t, int32(2) <= obs.eventCnt.Load() &&
		obs.eventCnt.Load() <= int32(3))
	for _, event := range *obs.recentEvents.Load() {
		assert.Contains(t, []discovery.Event{
			{
				Type: discovery.EventTypeRemove,
				Old: discovery.Instance{
					Name:  "McCarthy",
					Group: "Old",
					Mode:  discovery.ModeRW,
				},
			},
			{
				Type: discovery.EventTypeAdd,
				New: discovery.Instance{
					Name:  "McCarthy",
					Group: "New",
					Mode:  discovery.ModeRW,
				},
			},
		}, event)
	}
}

func TestConnectable_Unsubscribe_Concurrent(t *testing.T) {
	for i := 0; i < 100000; i++ {
		go func() {
			sub := newMockSubscriber()

			connectable := subscriber.NewConnectable(&mockDialerFactory{}, sub)
			assert.NotNil(t, connectable)

			obs := newMockObserver()
			err := connectable.Subscribe(context.Background(), obs)
			assert.NoError(t, err)

			wg := sync.WaitGroup{}
			wg.Add(2)

			go func() {
				connectable.Unsubscribe(obs)
				wg.Done()
			}()

			go func() {
				connectable.Unsubscribe(obs)
				wg.Done()
			}()
			wg.Wait()

			assert.Equal(t, int32(1), obs.errCnt.Load())
			assert.Equal(t, discovery.ErrUnsubscribe, obs.recentErr)
			assert.Equal(t, int32(0), obs.eventCnt.Load())
			assert.Equal(t, int32(1), sub.unsubCnt.Load())
		}()
	}
}

// Test to make sure there is no panic.
func TestConnectable_Unsubscribe_NilObserver(t *testing.T) {
	connectable := subscriber.NewConnectable(&mockDialerFactory{},
		newMockSubscriber())
	assert.NotNil(t, connectable)

	connectable.Unsubscribe(nil)
}

func TestConnectable_ReuseSchedule(t *testing.T) {
	connectable := subscriber.NewConnectable(&mockDialerFactory{},
		newMockSubscriber())
	assert.NotNil(t, connectable)

	obs := newMockObserver()
	err := connectable.Subscribe(context.Background(), obs)
	assert.NoError(t, err)

	connectable.Unsubscribe(obs)

	obs.errWg.Add(1)

	err = connectable.Subscribe(context.Background(), obs)
	assert.NoError(t, err)

	connectable.Unsubscribe(obs)
}
