.PHONY: build clean test

BUILD_DIR = build

build: $(BUILD_DIR)/counter.wasm

$(BUILD_DIR)/counter.wasm: counter.go
	@mkdir -p $(BUILD_DIR)
	tinygo build -o $(BUILD_DIR)/counter.wasm -target=wasi counter.go
	@echo "✓ Built counter.wasm"

clean:
	rm -rf $(BUILD_DIR)

test: build
	@echo "Testing WASM module..."
	wasm-validate $(BUILD_DIR)/counter.wasm
	@echo "✓ WASM module is valid"

.DEFAULT_GOAL := build
