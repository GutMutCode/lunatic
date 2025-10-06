.PHONY: build clean test counter all

BUILD_DIR = build

all: counter

build: all

counter: $(BUILD_DIR)/counter.wasm

$(BUILD_DIR)/counter.wasm: src/counter.c
	@mkdir -p $(BUILD_DIR)
	@if command -v emcc >/dev/null 2>&1; then \
		emcc src/counter.c -o $(BUILD_DIR)/counter.wasm -O3 -s STANDALONE_WASM=1 -s EXPORTED_FUNCTIONS='["_increment","_get_count","_reset","_start"]' --no-entry; \
		echo "✓ Built counter.wasm with Emscripten"; \
	else \
		echo "❌ Emscripten (emcc) not found. Please install Emscripten to build C examples."; \
		echo "   Installation: https://emscripten.org/docs/getting_started/downloads.html"; \
		exit 1; \
	fi

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