.PHONY: all build clean test check-tinygo guest_e2e guest-e2e \
	run_guest_e2e run-guest-e2e counter gen_server supervisor simulations

BUILD_DIR = build
TINYGO ?= tinygo
TINYGO_VERSION = 0.41.1
LUNATIC ?= ../../target/debug/lunatic

all: guest_e2e

build: guest_e2e

check-tinygo:
	@actual="$$($(TINYGO) version 2>/dev/null || true)"; \
	echo "$$actual" | grep -F "tinygo version $(TINYGO_VERSION) " >/dev/null || { \
		echo "TinyGo $(TINYGO_VERSION) is required (found: $${actual:-not installed})"; \
		exit 1; \
	}

guest_e2e: $(BUILD_DIR)/guest_e2e.wasm

guest-e2e: guest_e2e

$(BUILD_DIR)/guest_e2e.wasm: guest_e2e.go go.mod Makefile | check-tinygo
	@mkdir -p $(BUILD_DIR)
	$(TINYGO) build -o $@ -target=wasip1 -scheduler=none -opt=z -no-debug guest_e2e.go
	@echo "✓ Built guest_e2e.wasm with TinyGo $(TINYGO_VERSION) for WASI Preview 1"

run_guest_e2e: guest_e2e
	$(LUNATIC) run $(BUILD_DIR)/guest_e2e.wasm

run-guest-e2e: run_guest_e2e

# These are source-level Go/TinyGo simulations or generic Wasm examples. They
# do not exercise Lunatic process/message imports and are intentionally kept
# out of the default build and E2E target.
simulations: counter gen_server supervisor

counter: $(BUILD_DIR)/counter.wasm

gen_server: $(BUILD_DIR)/gen_server_example.wasm

supervisor: $(BUILD_DIR)/supervisor_example.wasm

$(BUILD_DIR)/counter.wasm: counter.go
	@mkdir -p $(BUILD_DIR)
	$(TINYGO) build -o $(BUILD_DIR)/counter.wasm -target=wasip1 counter.go
	@echo "✓ Built counter.wasm"

$(BUILD_DIR)/gen_server_example.wasm: gen_server_example.go
	@mkdir -p $(BUILD_DIR)
	$(TINYGO) build -o $(BUILD_DIR)/gen_server_example.wasm -target=wasip1 gen_server_example.go
	@echo "✓ Built gen_server_example.wasm"

$(BUILD_DIR)/supervisor_example.wasm: supervisor_example.go
	@mkdir -p $(BUILD_DIR)
	$(TINYGO) build -o $(BUILD_DIR)/supervisor_example.wasm -target=wasip1 supervisor_example.go
	@echo "✓ Built supervisor_example.wasm"

clean:
	rm -rf $(BUILD_DIR)

test: run_guest_e2e
	@echo "Validating the executed TinyGo guest module..."
	@if command -v wasm-validate >/dev/null 2>&1; then \
		wasm-validate $(BUILD_DIR)/guest_e2e.wasm && \
		echo "✓ guest_e2e.wasm is valid"; \
	else \
		echo "⚠ wasm-validate not found; the successful Lunatic execution remains the required check"; \
	fi

.DEFAULT_GOAL := all
