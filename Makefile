VERSION := $(shell sh -c 'git describe --always --tags')
BRANCH := $(shell sh -c 'git rev-parse --abbrev-ref HEAD')
COMMIT := $(shell sh -c 'git rev-parse --short HEAD')
ifdef GOBIN
PATH := $(GOBIN):$(PATH)
else
PATH := $(subst :,/bin:,$(GOPATH))/bin:$(PATH)
endif

# Standard Udup build
default: build

# Windows build
windows: build-windows

# Only run the build (no dependency grabbing)
build:
	go build -o udup -ldflags \
		"-X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.branch=$(BRANCH)" \
		./cmd/udup/main.go

build-windows:
	GOOS=windows GOARCH=amd64 go build -o udup.exe -ldflags \
		"-X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.branch=$(BRANCH)" \
		./cmd/udup/main.go

# run package script
package:
	./scripts/build.py --package --version="$(VERSION)" --platform=linux --arch=amd64 --no-get

# Run "short" unit tests
test-short: vet
	go test -short ./...

vet:
	go vet ./...

.PHONY: test-short vet build default
