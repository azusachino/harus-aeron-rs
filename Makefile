NIX_RUN := $(if $(IN_NIX_SHELL),,nix develop --command )
AERON_UPSTREAM_REPO ?= https://github.com/aeron-io/aeron.git
AERON_UPSTREAM_REF ?= release/1.50.x
AERON_UPSTREAM_DIR ?= vendor/aeron
TOKIO_UPSTREAM_REPO ?= https://github.com/tokio-rs/tokio.git
TOKIO_UPSTREAM_REF ?= master
TOKIO_UPSTREAM_DIR ?= vendor/tokio
RUST_UPSTREAM_REPO ?= https://github.com/rust-lang/rust.git
RUST_UPSTREAM_REF ?= stable
RUST_UPSTREAM_DIR ?= vendor/rust
INTEROP_SOAK_MESSAGES ?= 1000

.DEFAULT_GOAL := help

.PHONY: help fmt fmt-check build build-driver test test-unit test-integration test-py lint check clean run \
       tutorial-check bench stress \
       setup-upstream-aeron setup-upstream-tokio setup-upstream-rust \
       interop interop-smoke status test-parity

help:  ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "Usage: make \033[36m<target>\033[0m\n\n"} \
		/^[a-zA-Z0-9_-]+:.*?##/ {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

fmt:  ## Format Rust, config files, and scripts/
	$(NIX_RUN) cargo fmt --all
	$(NIX_RUN) prettier --write "**/*.{json,yaml,yml}"
	uv run --group dev ruff format scripts/

fmt-check:  ## Check formatting without writing
	$(NIX_RUN) cargo fmt --all -- --check
	$(NIX_RUN) prettier --check "**/*.{json,yaml,yml}"
	uv run --group dev ruff format --check scripts/
	uv run --group dev ruff check scripts/

build:  ## Build the main workspace
	$(NIX_RUN) cargo build --workspace

build-driver:  ## Build the driver binary only
	$(NIX_RUN) cargo build -p aeron-driver --bin aeron-driver

lint:  ## Run clippy across the workspace
	$(NIX_RUN) cargo clippy --workspace --all-targets -- -D warnings

test:  ## Run all tests (Rust workspace + scripts/)
	$(NIX_RUN) cargo test --workspace
	uv run --group dev pytest -q

test-py:  ## Run scripts/ tests only
	uv run --group dev pytest -q

test-unit:  ## Run unit tests only (lib targets)
	$(NIX_RUN) cargo test --workspace --lib

test-integration:  ## Run integration tests only
	$(NIX_RUN) cargo test --workspace --test '*'

check: fmt-check lint build test  ## fmt-check + lint + build + test

clean:  ## Remove build artifacts
	$(NIX_RUN) cargo clean

run:  ## Run the media driver
	$(NIX_RUN) cargo run -p aeron-driver --bin aeron-driver

tutorial-check:  ## Build the tutorial learner-stub workspace
	$(NIX_RUN) cargo build --manifest-path tutorial/Cargo.toml --workspace

bench:  ## Run benchmarks (once crates/*/benches exist)
	$(NIX_RUN) cargo bench --workspace

stress:  ## Run long-running soak scenarios (once test/stress exists)
	@echo "stress targets not yet implemented — see test/stress/"

setup-upstream-aeron:  ## Shadow-clone upstream Aeron into vendor/aeron
	git submodule update --init --depth 1 --remote $(AERON_UPSTREAM_DIR)

setup-upstream-tokio:  ## Shadow-clone upstream Tokio into vendor/tokio
	git submodule update --init --depth 1 --remote $(TOKIO_UPSTREAM_DIR)

setup-upstream-rust:  ## Shadow-clone upstream rust-lang/rust into vendor/rust (stdlib API reference)
	git submodule update --init --depth 1 --remote $(RUST_UPSTREAM_DIR)

test-parity:  ## Report Rust test-file parity against vendor/aeron's upstream test suite
	uv run scripts/upstream_tests.py

interop-smoke:  ## Run the Java<->Rust interop smoke test (not yet implemented)
	@echo "interop-smoke not yet implemented — see deploy/interop/"

interop:  ## Run the full interop suite (not yet implemented)
	@echo "interop not yet implemented — see deploy/interop/"

status:  ## Show repo + submodule status
	@git status --short
	@git submodule status
