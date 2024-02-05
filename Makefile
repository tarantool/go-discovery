SHELL := /bin/bash
TAGS :=

.PHONY: codespell
codespell:
	@echo "Running codespell"
	codespell

.PHONY: deps
deps: deps_codespell deps_format deps_generate deps_lint

.PHONY: deps_codespell
deps_codespell:
	pip3 install codespell

.PHONY: deps_format
deps_format:
	go install golang.org/x/tools/cmd/goimports@latest

.PHONY: deps_generate
deps_generate:
	go install golang.org/x/tools/cmd/stringer@latest

.PHONY: deps_lint
deps_lint:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.55.2

.PHONY: format
format:
	goimports -l -w .

.PHONY: generate
generate:
	go generate ./...

.PHONY: godoc_run
godoc_run:
	godoc -http=:6060

.PHONY: godoc_open
godoc_open:
	xdg-open http://localhost:6060/pkg/$(shell go list -f "{{.ImportPath}}")

.PHONY: lint
lint:
	golangci-lint --config .golangci.yml run --build-tags "$(TAGS)"

.PHONY: test
test:
	@echo "Running all packages tests"
	go clean -testcache
	go test -tags "$(TAGS)" ./... -v -p 1

.PHONY: testrace
testrace:
	@echo "Running all packages tests with data race detector"
	go clean -testcache
	go test -race -tags "$(TAGS)" ./... -v -p 1
