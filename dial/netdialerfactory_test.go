package dial_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tarantool/go-tarantool/v2"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/dial"
)

var _ discovery.DialerFactory = &dial.NetDialerFactory{}

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
		want        tarantool.Dialer
		wantErr     bool
	}{
		{
			name: "One uri test",
			factoryArgs: factoryArgs{
				"user",
				"password",
			},
			args: args{discovery.Instance{
				URI: []string{
					"localhost:3301",
				},
			}},
			want: dial.CompositeDialer{
				Dialers: []tarantool.Dialer{
					&tarantool.NetDialer{
						Address:  "localhost:3301",
						User:     "user",
						Password: "password",
					},
				},
			},
		},
		{
			name: "Multiple URI",
			factoryArgs: factoryArgs{
				"user",
				"pwd",
			},
			args: args{discovery.Instance{
				URI: []string{
					"localhost:3301",
					"unix://tmp/iproto.sock",
				},
			}},
			want: dial.CompositeDialer{
				Dialers: []tarantool.Dialer{
					&tarantool.NetDialer{
						Address:  "localhost:3301",
						User:     "user",
						Password: "pwd",
					},
					&tarantool.NetDialer{
						Address:  "unix://tmp/iproto.sock",
						User:     "user",
						Password: "pwd",
					},
				},
			},
		},
		{
			name: "No URIs",
			factoryArgs: factoryArgs{
				"user",
				"pwd",
			},
			args: args{discovery.Instance{
				URI: []string{},
			}},
			want:    tarantool.NetDialer{},
			wantErr: true,
		},
		{
			name: "Nil URIs",
			factoryArgs: factoryArgs{
				"user",
				"pwd",
			},
			args: args{discovery.Instance{
				URI: nil,
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

			factory := dial.NewNetDialerFactory(tt.factoryArgs.username,
				tt.factoryArgs.password,
				opts)
			got, gotOpts, err := factory.NewDialer(tt.args.instance)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, opts, gotOpts)
		})
	}
}
