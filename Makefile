# drainwatch
#
# Everything here is reproducible from a clean checkout with Docker, kind and Go
# installed. `make demo` is the one-command path; `make reproduce` is the
# three-arm experiment.

SHELL := /bin/bash

# VERSION comes from `git describe --tags`, so a build made at the tag reports
# exactly "0.1.0" and a build made after it reports "0.1.0-3-gabc1234". A report
# therefore states on its face whether it came from the released commit or from
# work on top of it, which a bare tag name would hide. Untagged checkouts fall
# back to the development version.
GIT_TAG     := $(shell git describe --tags 2>/dev/null)
VERSION     ?= $(if $(GIT_TAG),$(patsubst v%,%,$(GIT_TAG)),0.1.0-dev)
GIT_COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
GIT_DIRTY   := $(shell test -n "$$(git status --porcelain 2>/dev/null)" && echo "-dirty" || echo "")
COMMIT      := $(GIT_COMMIT)$(GIT_DIRTY)

MODULE      := github.com/jaynirmal15/drainwatch
LDFLAGS     := -X $(MODULE)/internal/report.Version=$(VERSION) -X $(MODULE)/internal/report.GitCommit=$(COMMIT)

BIN         := bin/drainwatch
# The image tag is stable and independent of VERSION; see DefaultProbeImage in
# internal/orchestrate. The build inside the image is stamped via ldflags and
# reported by the probe at startup.
IMAGE_TAG   ?= 0.1.0
IMAGE       := drainwatch-probe:$(IMAGE_TAG)
KIND_CLUSTER := drainwatch
KIND_CONFIG := deploy/kind/kind-config.yaml
OUT         ?= out

.PHONY: all build test fmt vet lint kind-up kind-down probe-image probe-image-build probe-image-load demo reproduce clean help

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
probe-image: probe-image-build probe-image-load

## probe-image-build: build the probe image only (no cluster needed)
probe-image-build:
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg GIT_COMMIT=$(COMMIT) \
		-t $(IMAGE) .

## probe-image-load: side-load an already-built probe image into kind
#  Split from the build so an experiment can freeze one image and load that same
#  image into each arm's fresh cluster, instead of rebuilding per arm and
#  silently recording arms from different binaries.
probe-image-load:
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
