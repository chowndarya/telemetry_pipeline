# ============================================================
# Telemetry Pipeline - Makefile
# ============================================================
# Provides unified targets for testing and code coverage
# across all components (streamer, collector, APIs).
# ============================================================

# Configuration
GO              := go
COVERAGE_DIR    := coverage
COVERAGE_FILE   := $(COVERAGE_DIR)/coverage.out
COVERAGE_HTML   := $(COVERAGE_DIR)/coverage.html
COVERAGE_TXT    := $(COVERAGE_DIR)/coverage.txt
PACKAGES        := ./...
INTEGRATION_TAG := integration

# Colors for terminal output
GREEN  := \033[0;32m
YELLOW := \033[0;33m
RED    := \033[0;31m
NC     := \033[0m

# Patterns to exclude from coverage (regex)
COVERAGE_EXCLUDE := (\.pb\.go|\.pb\.gw\.go|_mock\.go|mock_.*\.go|/mocks/|/grpc_proto/|/docs/|main\.go)

# Packages to test (excludes generated code directories)
PACKAGES := $(shell go list ./... | grep -v -E '/(mocks|grpc_proto|docs)$$')


# ============================================================
# Default target
# ============================================================
.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help message
	@echo "Telemetry Pipeline - Available targets:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'

# ============================================================
# Test targets
# ============================================================
.PHONY: test
test: ## Run all unit tests (no coverage)
	@echo "$(YELLOW)Running unit tests...$(NC)"
	$(GO) test -v $(PACKAGES)

.PHONY: test-short
test-short: ## Run only short tests
	@echo "$(YELLOW)Running short tests...$(NC)"
	$(GO) test -v -short $(PACKAGES)

.PHONY: test-integration
test-integration: ## Run integration tests (requires Docker)
	@echo "$(YELLOW)Running integration tests...$(NC)"
	$(GO) test -v -tags=$(INTEGRATION_TAG) -timeout 10m $(PACKAGES)

# ============================================================
# Coverage targets
# ============================================================
.PHONY: coverage
coverage: ## Run unit tests with coverage (excludes generated code)
	@echo "$(YELLOW)Running tests with coverage...$(NC)"
	@mkdir -p $(COVERAGE_DIR)
	$(GO) test -coverprofile=$(COVERAGE_FILE).tmp -covermode=atomic $(PACKAGES)
	@echo "$(YELLOW)Filtering excluded files...$(NC)"
	@awk 'NR==1 || !/(\.pb\.go|_mock\.go|mock_.*\.go|\/mocks\/|\/grpc_proto\/|\/docs\/)/' \
		$(COVERAGE_FILE).tmp > $(COVERAGE_FILE)
	@rm $(COVERAGE_FILE).tmp
	@echo ""
	@echo "$(GREEN)=== Coverage Summary ===$(NC)"
	$(GO) tool cover -func=$(COVERAGE_FILE) | tee $(COVERAGE_TXT)
	@echo ""
	@TOTAL=$$($(GO) tool cover -func=$(COVERAGE_FILE) | grep total | awk '{print $$3}'); \
	echo "$(GREEN)Total coverage: $$TOTAL$(NC)"

.PHONY: coverage-html
coverage-html: coverage ## Generate HTML coverage report and open in browser
	@echo "$(YELLOW)Generating HTML coverage report...$(NC)"
	$(GO) tool cover -html=$(COVERAGE_FILE) -o $(COVERAGE_HTML)
	@echo "$(GREEN)HTML report: $(COVERAGE_HTML)$(NC)"
	@if command -v open >/dev/null 2>&1; then \
		open $(COVERAGE_HTML); \
	elif command -v xdg-open >/dev/null 2>&1; then \
		xdg-open $(COVERAGE_HTML); \
	fi

.PHONY: coverage-full
coverage-full: ## Run unit + integration tests with coverage
	@echo "$(YELLOW)Running full coverage (unit + integration)...$(NC)"
	@mkdir -p $(COVERAGE_DIR)
	$(GO) test -v -tags=$(INTEGRATION_TAG) -timeout 10m \
		-coverprofile=$(COVERAGE_FILE) -covermode=atomic $(PACKAGES)
	@echo ""
	@echo "$(GREEN)=== Coverage Summary ===$(NC)"
	$(GO) tool cover -func=$(COVERAGE_FILE) | tee $(COVERAGE_TXT)

.PHONY: coverage-by-package
coverage-by-package: ## Show coverage broken down by package
	@echo "$(YELLOW)Per-package coverage:$(NC)"
	@mkdir -p $(COVERAGE_DIR)
	$(GO) test -coverprofile=$(COVERAGE_FILE) -covermode=atomic $(PACKAGES) 2>/dev/null
	@$(GO) tool cover -func=$(COVERAGE_FILE) | grep -v "^total" | \
		awk '{print $$1}' | sed 's|/[^/]*$$||' | sort -u | while read pkg; do \
			cov=$$($(GO) test -cover $$pkg 2>/dev/null | grep -oE 'coverage: [0-9.]+%' || echo "no tests"); \
			printf "  %-60s %s\n" "$$pkg" "$$cov"; \
		done

.PHONY: coverage-threshold
coverage-threshold: coverage ## Fail if coverage is below threshold (default 70%)
	@THRESHOLD=$${COVERAGE_THRESHOLD:-70}; \
	TOTAL=$$($(GO) tool cover -func=$(COVERAGE_FILE) | grep total | awk '{print $$3}' | sed 's/%//'); \
	echo "Coverage: $$TOTAL% (threshold: $$THRESHOLD%)"; \
	if awk "BEGIN {exit !($$TOTAL < $$THRESHOLD)}"; then \
		echo "$(RED)❌ Coverage $$TOTAL% is below threshold $$THRESHOLD%$(NC)"; \
		exit 1; \
	else \
		echo "$(GREEN)✅ Coverage $$TOTAL% meets threshold $$THRESHOLD%$(NC)"; \
	fi

# ============================================================
# Per-component targets (useful for focused development)
# ============================================================
.PHONY: test-streamer
test-streamer: ## Test only the streamer component
	@echo "$(YELLOW)Testing streamer...$(NC)"
	$(GO) test -v -cover ./telemetry-streamer/...

.PHONY: test-collector
test-collector: ## Test only the collector component
	@echo "$(YELLOW)Testing collector...$(NC)"
	$(GO) test -v -cover ./telemetry-collector/...

.PHONY: test-apis
test-apis: ## Test only the API component
	@echo "$(YELLOW)Testing APIs...$(NC)"
	$(GO) test -v -cover ./telemetry_apis/...

# ============================================================
# Utility targets
# ============================================================
.PHONY: deps
deps: ## Install/update Go dependencies
	@echo "$(YELLOW)Installing dependencies...$(NC)"
	$(GO) mod download
	$(GO) mod tidy

.PHONY: mocks
mocks: ## Regenerate gomock files
	@echo "$(YELLOW)Generating mocks...$(NC)"
	mockgen -destination=mocks/mock_telemetry_client.go -package=mocks \
		github.com/chowndarya/telemetry_pipeline/grpc_proto \
		TelemetryServiceClient,TelemetryService_CollectTelemetryClient

.PHONY: clean-cov
clean-cov: ## Remove coverage files and test artifacts
	@echo "$(YELLOW)Cleaning...$(NC)"
	rm -rf $(COVERAGE_DIR)
	$(GO) clean -testcache

.PHONY: lint
lint: ## Run go vet
	@echo "$(YELLOW)Running go vet...$(NC)"
	$(GO) vet $(PACKAGES)

.PHONY: ci
ci: deps lint coverage coverage-threshold ## CI pipeline target (deps + lint + coverage + threshold)
	@echo "$(GREEN)✅ CI pipeline passed$(NC)"


# ============================================================
# Deployment
# ============================================================

# Makefile for building, pushing images and deploying with Helm

# Image tags
TELEMETRY_API_IMAGE=localhost/telemetry-apis:latest
TELEMETRY_QUEUE_IMAGE=localhost/telemetry-queue:latest
TELEMETRY_COLLECTOR_IMAGE=localhost/telemetry-collector:latest
TELEMETRY_STREAMER_IMAGE=localhost/telemetry-streamer:latest

# Helm release and namespace
HELM_RELEASE=gpu-telemetry
HELM_NAMESPACE=gpu-telemetry
HELM_CHART_PATH=./gpu-telemetry-chart

PLATFORM ?= $(shell uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')

.PHONY: all build push load deploy clean

all: build push load deploy

build:
	podman build --platform=linux/$(PLATFORM) -t $(TELEMETRY_API_IMAGE) -f ./telemetry_apis/Dockerfile .
	podman build --platform=linux/$(PLATFORM) -t $(TELEMETRY_QUEUE_IMAGE) -f ./telemetry_queue/Dockerfile .
	podman build --platform=linux/$(PLATFORM) -t $(TELEMETRY_COLLECTOR_IMAGE) -f ./telemetry_collector/Dockerfile .
	podman build --platform=linux/$(PLATFORM) -t $(TELEMETRY_STREAMER_IMAGE) -f ./telemetry_streamer/Dockerfile .

push:
	podman save $(TELEMETRY_API_IMAGE) -o telemetry-apis.tar
	podman save $(TELEMETRY_QUEUE_IMAGE) -o telemetry-queue.tar
	podman save $(TELEMETRY_COLLECTOR_IMAGE) -o telemetry-collector.tar
	podman save $(TELEMETRY_STREAMER_IMAGE) -o telemetry-streamer.tar

load:
	kind load image-archive telemetry-apis.tar --name kind-cluster
	kind load image-archive telemetry-queue.tar --name kind-cluster
	kind load image-archive telemetry-collector.tar --name kind-cluster
	kind load image-archive telemetry-streamer.tar --name kind-cluster

deploy:
	helm upgrade --install $(HELM_RELEASE) $(HELM_CHART_PATH) --namespace $(HELM_NAMESPACE) --create-namespace

clean:
	rm -f telemetry-apis.tar telemetry-queue.tar telemetry-collector.tar telemetry-streamer.tar


# ============================================================
# OpenAPI spec generation
# ============================================================

# Path to the telemetry-apis directory
TELEMETRY_APIS_DIR = ./telemetry_apis

.PHONY: openapi 
# Generates OpenAPI 2.0 (Swagger) spec using swag
openapi:
	@echo "Generating OpenAPI spec..."
	@which swag > /dev/null || (echo "swag not found. Installing..." && go install github.com/swaggo/swag/cmd/swag@latest)
	cd $(TELEMETRY_APIS_DIR) && swag init -g main.go -o ./docs
	@echo "OpenAPI spec generated at $(TELEMETRY_APIS_DIR)/docs/"
	@echo "  - swagger.json (OpenAPI 2.0 JSON)"
	@echo "  - swagger.yaml (OpenAPI 2.0 YAML)"

# ============================================================
# Stress Test
# ============================================================


.PHONY: test-stress
test-stress: ## Run stress tests (10 producers + 10 collectors)
	@echo "$(YELLOW)Running stress tests...$(NC)"
	$(GO) test -v -race -timeout 5m -run "^TestStress" ./telemetry_queue/...

.PHONY: test-stress-bench
test-stress-bench: ## Run stress tests with CPU profiling
	@echo "$(YELLOW)Running stress tests with profiling...$(NC)"
	$(GO) test -v -race -timeout 5m -run "^TestStress" \
		-cpuprofile=cpu.prof -memprofile=mem.prof \
		./telemetry_queue/...
	@echo "$(GREEN)Profiles: cpu.prof, mem.prof$(NC)"
	@echo "View with: go tool pprof cpu.prof"
