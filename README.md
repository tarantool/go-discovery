# discovery

Package discovery implements cluster discovery helpers for Tarantool 3.0
implemented in Go language according to the [design document][design-document].

# Run tests

To run default set of tests:

```shell
go test -v ./...
```

# Development

To generate code, you need to install two utilities:

```shell
go install golang.org/x/tools/cmd/stringer@latest
go install github.com/posener/goreadme/cmd/goreadme@latest
```

After that, you could generate the code.

```shell
go generate ./...
```

In addition, you need to install linter:

```shell
go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.51.1
```

You could run the linter with the command.

```shell
golangci-lint --config .golangci.yml run
```

Finally, codespell:

```shell
pip3 install codespell
codespell
```

You could also use our Makefile targets:

```shell
make deps
make format
make generate
make lint
make test
make testrace
```

# Documentation

You could run the `godoc` server on `localhost:6060` with the command:

```shell
make godoc_run
```

And open the generated documentation in another terminal or use the
[link][godoc-link]:

```shell
make godoc_open
```

[design-document]: https://www.notion.so/Cluster-discovery-Go-library-3613a0bd7e3a439d86f99c083d9d8ce4
[godoc-link]: http://localhost:6060/pkg/github.com/tarantool/go-discovery
