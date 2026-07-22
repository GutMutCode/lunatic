.PHONY: all build clean test guest-e2e guest-e2e-test host-examples

GUEST_E2E_TARGET = wasm32-unknown-unknown
GUEST_E2E_MANIFEST = guest-e2e/Cargo.toml
GUEST_E2E_WASM = guest-e2e/target/$(GUEST_E2E_TARGET)/release/lunatic_rust_guest_e2e.wasm
LUNATIC ?= ../../target/debug/lunatic
PINNED_CARGO ?= cargo +1.94.0

all: guest-e2e

build: guest-e2e

test: guest-e2e-test

guest-e2e: $(GUEST_E2E_WASM)

guest-e2e-test: guest-e2e
	$(LUNATIC) run $(GUEST_E2E_WASM)

host-examples:
	cargo check --locked --manifest-path Cargo.toml

$(GUEST_E2E_WASM): guest-e2e/src/lib.rs guest-e2e/Cargo.toml guest-e2e/Cargo.lock Makefile
	$(PINNED_CARGO) build --locked --manifest-path $(GUEST_E2E_MANIFEST) --target=$(GUEST_E2E_TARGET) --release
	@echo "Built $(GUEST_E2E_WASM)"

clean:
	$(PINNED_CARGO) clean --manifest-path $(GUEST_E2E_MANIFEST)

.DEFAULT_GOAL := all
