.PHONY: build clean test counter all

BUILD_DIR = build
TARGET = wasm32-freestanding

all: counter

build: all

counter: $(BUILD_DIR)/counter.wasm

$(BUILD_DIR)/counter.wasm: src/counter.zig
	@mkdir -p $(BUILD_DIR)
	zig build-exe -target $(TARGET) -O ReleaseSmall src/counter.zig -femit-bin=$(BUILD_DIR)/counter.wasm
	@echo "✓ Built counter.wasm"

clean:
	rm -rf $(BUILD_DIR)

test: build
	@echo "Testing WASM module..."
	@if command -v wasm-validate >/dev/null 2>&1; then \
		wasm-validate $(BUILD_DIR)/counter.wasm && \
		echo "✓ WASM module is valid"; \
	else \
		echo "⚠ wasm-validate not found, skipping validation"; \
	fi

.DEFAULT_GOAL := all