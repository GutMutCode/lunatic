.PHONY: build clean test counter gen_server supervisor

BUILD_DIR = build

all: counter gen_server supervisor

build: all

counter: $(BUILD_DIR)/counter.wasm

gen_server: $(BUILD_DIR)/gen_server_example.wasm

supervisor: $(BUILD_DIR)/supervisor_example.wasm

$(BUILD_DIR)/counter.wasm: counter.go
	@mkdir -p $(BUILD_DIR)
	tinygo build -o $(BUILD_DIR)/counter.wasm -target=wasi counter.go
	@echo "✓ Built counter.wasm"

$(BUILD_DIR)/gen_server_example.wasm: gen_server_example.go
	@mkdir -p $(BUILD_DIR)
	tinygo build -o $(BUILD_DIR)/gen_server_example.wasm -target=wasi gen_server_example.go
	@echo "✓ Built gen_server_example.wasm"

$(BUILD_DIR)/supervisor_example.wasm: supervisor_example.go
	@mkdir -p $(BUILD_DIR)
	tinygo build -o $(BUILD_DIR)/supervisor_example.wasm -target=wasi supervisor_example.go
	@echo "✓ Built supervisor_example.wasm"

clean:
	rm -rf $(BUILD_DIR)

test: build
	@echo "Testing WASM modules..."
	@if command -v wasm-validate >/dev/null 2>&1; then \
		wasm-validate $(BUILD_DIR)/counter.wasm && \
		wasm-validate $(BUILD_DIR)/gen_server_example.wasm && \
		wasm-validate $(BUILD_DIR)/supervisor_example.wasm && \
		echo "✓ All WASM modules are valid"; \
	else \
		echo "⚠ wasm-validate not found, skipping validation"; \
	fi

.DEFAULT_GOAL := all
