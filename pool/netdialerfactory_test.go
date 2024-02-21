package pool_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/pool"
	"github.com/tarantool/go-tarantool/v2"
)

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
				URI: []string{
					"localhost:3301",
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
				URI: []string{
					"localhost:3301",
					"unix://tmp/iproto.sock",
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
				URI: []string{
					"localhost:3301",
					"localhost:3302",
				},
			}},
			want: tarantool.NetDialer{
				Address:  "localhost:3301",
				User:     "user",
				Password: "pwd",
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := pool.NewNetDialerFactory(tt.factoryArgs.username,
				tt.factoryArgs.password)
			got, err := factory.NewDialer(tt.args.instance)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			gotNet, ok := got.(*tarantool.NetDialer)
			require.True(t, ok)
			assert.Equal(t, tt.want, *gotNet)
		})
	}
}
