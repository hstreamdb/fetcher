PACKAGE := github.com/hstreamdb/fetcher

export GO_BUILD=GO111MODULE=on CGO_ENABLED=0 GOOS=$(GOOS) go build -ldflags '-s -w'

all: fetcher

fmt:
	gofmt -s -w -l `find . -name '*.go' -print`

fetcher:
	$(GO_BUILD) -o bin/fetcher $(PACKAGE)

.PHONY: fmt, fetcher all

