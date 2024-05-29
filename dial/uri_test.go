package dial_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/tarantool/go-discovery"
	"github.com/tarantool/go-discovery/dial"
)

func TestGet(t *testing.T) {
	type args struct {
		instance discovery.Instance
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Single URI",
			args: args{discovery.Instance{
				URI: []string{
					"localhost:3301",
				},
			}},
			want: "localhost:3301",
		},
		{
			name: "Prefer Unix socket",
			args: args{discovery.Instance{
				URI: []string{
					"localhost:3301",
					"unix://tmp/iproto.sock",
				},
			}},
			want: "unix://tmp/iproto.sock",
		},
		{
			name: "Take first URI",
			args: args{discovery.Instance{
				URI: []string{
					"localhost:3301",
					"localhost:3302",
				},
			}},
			want: "localhost:3301",
		},
		{
			name: "No URIs",
			args: args{discovery.Instance{
				URI: []string{},
			}},
			wantErr: true,
		},
		{
			name: "Nil URIs",
			args: args{discovery.Instance{
				URI: nil,
			}},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := dial.GetURIPreferUnix(tt.args.instance)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
