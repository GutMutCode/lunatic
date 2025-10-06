.PHONY: build clean test counter echo all

BUILD_DIR = build
TARGET = wasm32-wasi

all: counter echo

build: all

counter: $(BUILD_DIR)/counter.wasm

echo: $(BUILD_DIR)/echo_server.wasm

$(BUILD_DIR)/counter.wasm: src/counter.rs Cargo.toml
	@mkdir -p $(BUILD_DIR)
	cargo build --target=$(TARGET) --release --bin counter
	cp target/$(TARGET)/release/counter.wasm $(BUILD_DIR)/
	@echo "✓ Built counter.wasm"

$(BUILD_DIR)/echo_server.wasm: src/echo_server.rs Cargo.toml
	@mkdir -p $(BUILD_DIR)
	cargo build --target=$(TARGET) --release --bin echo_server
	cp target/$(TARGET)/release/echo_server.wasm $(BUILD_DIR)/
	@echo "✓ Built echo_server.wasm"

clean:
	cargo clean
	rm -rf $(BUILD_DIR)

test: build
	@echo "Testing WASM modules..."
	@if command -v wasm-validate >/dev/null 2>&1; then \
		wasm-validate $(BUILD_DIR)/counter.wasm && \
		wasm-validate $(BUILD_DIR)/echo_server.wasm && \
		echo "✓ All WASM modules are valid"; \
	else \
		echo "⚠ wasm-validate not found, skipping validation"; \
	fi

.DEFAULT_GOAL := all
