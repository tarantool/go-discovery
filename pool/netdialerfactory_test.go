package pool_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/pool"
	"github.com/tarantool/go-tarantool/v2"
)

var _ pool.DialerFactory = &pool.NetDialerFactory{}

func TestNetDialerFactory_NewDialer(t *testing.T) {
	type factoryArgs struct {
		username string
		password string
	}
	type args struct {
		instance discovery.Instance
	}
	tests := []struct {
		name        string
		factoryArgs factoryArgs
		args        args
		want        tarantool.NetDialer
		wantErr     bool
	}{
		{
			name: "One uri test",
			factoryArgs: factoryArgs{
				"user",
				"password",
			},
			args: args{discovery.Instance{
				Endpoints: []discovery.Endpoint{
					discovery.Endpoint{
						URI:       "localhost:3301",
						Transport: discovery.TransportPlain,
					},
				},
			}},
			want: tarantool.NetDialer{
				Address:  "localhost:3301",
				User:     "user",
				Password: "password",
			},
		},
		{
			name: "Select unit first",
			factoryArgs: factoryArgs{
				"user",
				"pwd",
			},
			args: args{discovery.Instance{
				Endpoints: []discovery.Endpoint{
					discovery.Endpoint{
						URI:       "localhost:3301",
						Transport: discovery.TransportPlain,
					},
					discovery.Endpoint{
						URI:       "unix://tmp/iproto.sock",
						Transport: discovery.TransportPlain,
					},
				},
			}},
			want: tarantool.NetDialer{
				Address:  "unix://tmp/iproto.sock",
				User:     "user",
				Password: "pwd",
			},
		},
		{
			name: "Take first uri",
			factoryArgs: factoryArgs{
				"user",
				"pwd",
			},
			args: args{discovery.Instance{
				Endpoints: []discovery.Endpoint{
					discovery.Endpoint{
						URI:       "localhost:3301",
						Transport: discovery.TransportPlain,
					},
					discovery.Endpoint{
						URI:       "localhost:3302",
						Transport: discovery.TransportPlain,
					},
				},
			}},
			want: tarantool.NetDialer{
				Address:  "localhost:3301",
				User:     "user",
				Password: "pwd",
			},
		},
		{
			name: "Skip SSL transport",
			factoryArgs: factoryArgs{
				"user",
				"pwd",
			},
			args: args{discovery.Instance{
				Endpoints: []discovery.Endpoint{
					discovery.Endpoint{
						URI:       "localhost:3301",
						Transport: discovery.TransportSSL,
					},
					discovery.Endpoint{
						URI:       "localhost:3302",
						Transport: discovery.TransportPlain,
					},
				},
			}},
			want: tarantool.NetDialer{
				Address:  "localhost:3302",
				User:     "user",
				Password: "pwd",
			},
		},
		{
			name: "Skip unix with SSL transport",
			factoryArgs: factoryArgs{
				"user",
				"pwd",
			},
			args: args{discovery.Instance{
				Endpoints: []discovery.Endpoint{
					discovery.Endpoint{
						URI:       "unix://tmp/iproto.sock",
						Transport: discovery.TransportSSL,
					},
					discovery.Endpoint{
						URI:       "localhost:3302",
						Transport: discovery.TransportPlain,
					},
				},
			}},
			want: tarantool.NetDialer{
				Address:  "localhost:3302",
				User:     "user",
				Password: "pwd",
			},
		},
		{
			name: "Empty Endpoints",
			factoryArgs: factoryArgs{
				"user",
				"pwd",
			},
			args: args{discovery.Instance{
				Endpoints: []discovery.Endpoint{},
			}},
			want:    tarantool.NetDialer{},
			wantErr: true,
		},
		{
			name: "Nil Endpoints",
			factoryArgs: factoryArgs{
				"user",
				"pwd",
			},
			args: args{discovery.Instance{
				Endpoints: nil,
			}},
			want:    tarantool.NetDialer{},
			wantErr: true,
		},
		{
			name: "No plain transport",
			factoryArgs: factoryArgs{
				"user",
				"pwd",
			},
			args: args{discovery.Instance{
				Endpoints: []discovery.Endpoint{
					discovery.Endpoint{
						URI:       "localhost:3301",
						Transport: discovery.TransportSSL,
					},
					discovery.Endpoint{
						URI:       "localhost:3302",
						Transport: discovery.TransportSSL,
					},
				},
			}},
			want:    tarantool.NetDialer{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := tarantool.Opts{
				Timeout: time.Second,
			}

			factory := pool.NewNetDialerFactory(tt.factoryArgs.username,
				tt.factoryArgs.password,
				opts)
			got, gotOpts, err := factory.NewDialer(tt.args.instance)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			gotNet, ok := got.(*tarantool.NetDialer)
			require.True(t, ok)
			assert.Equal(t, tt.want, *gotNet)
			assert.Equal(t, opts, gotOpts)
		})
	}
}
