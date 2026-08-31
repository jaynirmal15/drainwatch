# drainwatch
#
# Everything here is reproducible from a clean checkout with Docker, kind and Go
# installed. `make demo` is the one-command path; `make reproduce` is the
# three-arm experiment.

SHELL := /bin/bash

VERSION     ?= 0.1.0
GIT_COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
GIT_DIRTY   := $(shell test -n "$$(git status --porcelain 2>/dev/null)" && echo "-dirty" || echo "")
COMMIT      := $(GIT_COMMIT)$(GIT_DIRTY)

MODULE      := github.com/jaynirmal15/drainwatch
LDFLAGS     := -X $(MODULE)/internal/report.Version=$(VERSION) -X $(MODULE)/internal/report.GitCommit=$(COMMIT)

BIN         := bin/drainwatch
IMAGE       := drainwatch-probe:$(VERSION)
KIND_CLUSTER := drainwatch
KIND_CONFIG := deploy/kind/kind-config.yaml
OUT         ?= out

.PHONY: all build test fmt vet lint kind-up kind-down probe-image demo reproduce clean help

all: build

## build: build the drainwatch binary with version and commit stamped in
build:
	@mkdir -p bin
	go build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN) ./cmd/drainwatch
	@echo "built $(BIN) ($(VERSION) $(COMMIT))"

## test: unit tests (flow state machine, outcome classification, report schema)
test:
	go test ./... -count=1

## fmt: gofmt every package
fmt:
	gofmt -l -w cmd internal

## vet: go vet
vet:
	go vet ./...

## lint: fmt check plus vet, for CI
lint:
	@test -z "$$(gofmt -l cmd internal)" || { echo "gofmt needed:"; gofmt -l cmd internal; exit 1; }
	go vet ./...

## probe-image: build the scratch probe image and side-load it into kind
probe-image:
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg GIT_COMMIT=$(COMMIT) \
		-t $(IMAGE) .
	kind load docker-image $(IMAGE) --name $(KIND_CLUSTER)

## kind-up: create the two-node kind cluster (idempotent) and load the image
kind-up:
	@if kind get clusters 2>/dev/null | grep -qx "$(KIND_CLUSTER)"; then \
		echo "kind cluster $(KIND_CLUSTER) already exists"; \
	else \
		kind create cluster --name $(KIND_CLUSTER) --config $(KIND_CONFIG) --wait 120s; \
	fi
	@$(MAKE) --no-print-directory probe-image

## kind-down: delete the kind cluster
kind-down:
	kind delete cluster --name $(KIND_CLUSTER)

## demo: kind-up, one trial with defaults, table on stdout, report in ./out
demo: build kind-up
	./scripts/dwrun.sh --out $(OUT)
	@echo
	@echo "report:  $(OUT)/report.json"
	@echo "summary: $(OUT)/summary.json"

## reproduce: clean cluster, three trials (drain / exit-now / ignore), summary
reproduce:
	./scripts/reproduce.sh

## clean: remove build output and reports
clean:
	rm -rf bin $(OUT)

## help: list targets
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## //' | awk -F': ' '{printf "  %-14s %s\n", $$1, $$2}'
