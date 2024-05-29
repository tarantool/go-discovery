package dial

import (
	"fmt"
	"strings"

	"golang.org/x/exp/slices"

	"github.com/tarantool/go-discovery"
)

func GetURIPreferUnix(instance discovery.Instance) (string, error) {
	if len(instance.URI) == 0 {
		return "", fmt.Errorf("%s instance URI list is empty", instance.Name)
	}

	// First look for unix-socket URI.
	var uri string
	if uriIndex := slices.IndexFunc(instance.URI, func(uri string) bool {
		return strings.HasPrefix(uri, "unix://") ||
			strings.HasPrefix(uri, "unix:") ||
			strings.HasPrefix(uri, "unix/:")
	}); uriIndex == -1 {
		uri = instance.URI[0]
	} else {
		uri = instance.URI[uriIndex]
	}

	return uri, nil
}
