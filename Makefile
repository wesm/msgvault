# Makefile for msgvault

.DEFAULT_GOAL := help

VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

LDFLAGS := -X github.com/wesm/msgvault/cmd/msgvault/cmd.Version=$(VERSION) \
           -X github.com/wesm/msgvault/cmd/msgvault/cmd.Commit=$(COMMIT) \
           -X github.com/wesm/msgvault/cmd/msgvault/cmd.BuildDate=$(BUILD_DATE)

LDFLAGS_RELEASE := $(LDFLAGS) -s -w

.PHONY: build build-release install clean test test-v fmt lint tidy shootout run-shootout help \
        docker-build docker-build-multi docker-run docker-clean

# Build the binary (debug)
build:
	CGO_ENABLED=1 go build -tags fts5 -ldflags="$(LDFLAGS)" -o msgvault ./cmd/msgvault
	@chmod +x msgvault

# Build with optimizations (release)
build-release:
	CGO_ENABLED=1 go build -tags fts5 -ldflags="$(LDFLAGS_RELEASE)" -trimpath -o msgvault ./cmd/msgvault
	@chmod +x msgvault

# Install to ~/.local/bin, $GOBIN, or $GOPATH/bin
install:
	@if [ -d "$(HOME)/.local/bin" ]; then \
		echo "Installing to ~/.local/bin/msgvault"; \
		CGO_ENABLED=1 go build -tags fts5 -ldflags="$(LDFLAGS)" -o "$(HOME)/.local/bin/msgvault" ./cmd/msgvault; \
	else \
		INSTALL_DIR="$${GOBIN:-$$(go env GOBIN)}"; \
		if [ -z "$$INSTALL_DIR" ]; then \
			GOPATH_FIRST="$$(go env GOPATH | cut -d: -f1)"; \
			INSTALL_DIR="$$GOPATH_FIRST/bin"; \
		fi; \
		mkdir -p "$$INSTALL_DIR"; \
		echo "Installing to $$INSTALL_DIR/msgvault"; \
		CGO_ENABLED=1 go build -tags fts5 -ldflags="$(LDFLAGS)" -o "$$INSTALL_DIR/msgvault" ./cmd/msgvault; \
	fi

# Clean build artifacts
clean:
	rm -f msgvault mimeshootout
	rm -rf bin/

# Run tests
test:
	go test -tags fts5 ./...

# Run tests with verbose output
test-v:
	go test -tags fts5 -v ./...

# Format code
fmt:
	go fmt ./...

# Run linter
lint:
	@which golangci-lint > /dev/null || (echo "Install golangci-lint: https://golangci-lint.run/usage/install/" && exit 1)
	golangci-lint run ./...

# Tidy dependencies
tidy:
	go mod tidy

# Build the MIME shootout tool
shootout:
	CGO_ENABLED=1 go build -o mimeshootout ./scripts/mimeshootout

# Run MIME shootout
run-shootout: shootout
	./mimeshootout -limit 1000

# Build Docker image (local architecture)
docker-build:
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg BUILD_DATE=$(BUILD_DATE) \
		-t msgvault:dev .

# Build Docker image for multiple architectures (requires buildx)
docker-build-multi:
	docker buildx build \
		--platform linux/amd64,linux/arm64 \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg BUILD_DATE=$(BUILD_DATE) \
		-t msgvault:dev .

# Run Docker container (interactive, for testing)
docker-run:
	docker run -it --rm \
		-v $$(pwd)/data:/data \
		-p 8080:8080 \
		-e MSGVAULT_API_KEY=dev-key \
		msgvault:dev serve

# Clean Docker images
docker-clean:
	docker rmi msgvault:dev 2>/dev/null || true
	docker rmi msgvault:test 2>/dev/null || true

# Show help
help:
	@echo "msgvault build targets:"
	@echo ""
	@echo "  build          - Debug build"
	@echo "  build-release  - Release build (optimized, stripped)"
	@echo "  install        - Install to ~/.local/bin or GOPATH"
	@echo ""
	@echo "  test           - Run tests"
	@echo "  test-v         - Run tests (verbose)"
	@echo "  fmt            - Format code"
	@echo "  lint           - Run linter"
	@echo "  tidy           - Tidy go.mod"
	@echo "  clean          - Remove build artifacts"
	@echo ""
	@echo "  docker-build       - Build Docker image (local arch)"
	@echo "  docker-build-multi - Build multi-arch Docker image"
	@echo "  docker-run         - Run Docker container (interactive)"
	@echo "  docker-clean       - Remove Docker images"
	@echo ""
	@echo "  shootout       - Build MIME shootout tool"
	@echo "  run-shootout   - Run MIME shootout"
