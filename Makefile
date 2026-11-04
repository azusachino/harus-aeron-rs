NIX_RUN := $(if $(IN_NIX_SHELL),,nix develop --command )
AERON_UPSTREAM_REPO ?= https://github.com/aeron-io/aeron.git
AERON_UPSTREAM_REF ?= release/1.50.x
AERON_UPSTREAM_DIR ?= vendor/aeron
# Pinned to the exact tag matching workspace.dependencies.tokio in Cargo.toml — not a
# tracked branch, since "master" is unreleased/bleeding-edge. Bump both together.
TOKIO_UPSTREAM_REPO ?= https://github.com/tokio-rs/tokio.git
TOKIO_UPSTREAM_REF ?= tokio-1.53.1
TOKIO_UPSTREAM_DIR ?= vendor/tokio
INTEROP_SOAK_MESSAGES ?= 1000

.DEFAULT_GOAL := help

.PHONY: help fmt fmt-check build build-driver test test-unit test-integration test-py lint check clean run \
       tutorial-check bench fuzz fuzz-check stress miri \
       setup-upstream-aeron setup-upstream-tokio \
       interop interop-smoke interop-soak interop-rust-smoke interop-rust-soak cluster-failover-repro status test-parity \
       astro-setup astro-dev astro-check astro-build astro-e2e

help:  ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "Usage: make \033[36m<target>\033[0m\n\n"} \
		/^[a-zA-Z0-9_-]+:.*?##/ {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

fmt:  ## Format Rust, config files, and scripts/
	$(NIX_RUN) cargo fmt --all
	mise exec -- bun run prettier --plugin prettier-plugin-astro --write "**/*.{json,yaml,yml,ts,css,mjs,astro}"
	uv run --group dev ruff format scripts/

fmt-check:  ## Check formatting without writing
	$(NIX_RUN) cargo fmt --all -- --check
	mise exec -- bun run prettier --plugin prettier-plugin-astro --check "**/*.{json,yaml,yml,ts,css,mjs,astro}"
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

check: fmt-check lint build test tutorial-check  ## fmt-check + lint + build + test + tutorial-check (mirrors CI)

clean:  ## Remove build artifacts
	$(NIX_RUN) cargo clean

run:  ## Run the media driver
	$(NIX_RUN) cargo run -p aeron-driver --bin aeron-driver

tutorial-check:  ## Build the tutorial learner-stub workspace
	$(NIX_RUN) cargo build --manifest-path tutorial/Cargo.toml --workspace

astro-setup:  ## Install the pinned Bun and locked Astro dependencies
	mise install
	mise exec -- bun install --frozen-lockfile

astro-dev:  ## Preview the Astro documentation site locally
	mise exec -- bun run astro:dev

astro-check: astro-setup  ## Type-check the Astro site and verify Markdown link rewriting
	mise exec -- bun run astro:check

astro-build: astro-setup  ## Type-check and build the Astro documentation site
	mise exec -- bun run astro:build

astro-e2e: astro-build  ## Verify documentation journeys in headless Chromium
	mise exec -- bun run astro:e2e

bench:  ## Run benchmarks (once crates/*/benches exist)
	$(NIX_RUN) cargo bench --workspace

fuzz-check:  ## Format and compile the libFuzzer targets without running them
	$(NIX_RUN) cargo fmt --manifest-path fuzz/Cargo.toml -- --check
	$(NIX_RUN) env RUSTFLAGS="--cfg fuzzing" cargo build --manifest-path fuzz/Cargo.toml

FUZZ_TIME ?= 10
FUZZ_TARGETS := frame_decode uri_parse ring_buffer_read log_buffer_read

fuzz: fuzz-check  ## Run each libFuzzer target for FUZZ_TIME seconds
	@command -v cargo-fuzz >/dev/null || { echo "cargo-fuzz is required; install it with cargo install cargo-fuzz"; exit 1; }
	@for target in $(FUZZ_TARGETS); do \
		cargo fuzz run $$target -- -max_total_time=$(FUZZ_TIME); \
	done

stress: interop-rust-soak cluster-failover-repro  ## Run bounded process soak and cluster failover gates

setup-upstream-aeron:  ## Shadow-clone upstream Aeron into vendor/aeron
	git submodule update --init --depth 1 --remote $(AERON_UPSTREAM_DIR)

setup-upstream-tokio:  ## Fetch and pin vendor/tokio to TOKIO_UPSTREAM_REF (bump together with the tokio Cargo.toml version)
	git -C $(TOKIO_UPSTREAM_DIR) fetch origin tag $(TOKIO_UPSTREAM_REF) --no-tags
	git -C $(TOKIO_UPSTREAM_DIR) checkout $(TOKIO_UPSTREAM_REF)

miri:  ## Run Miri UB checks on crates with unsafe code (requires: rustup +nightly component add miri)
	cargo +nightly miri test -p agrona

test-parity:  ## Report Rust test-file parity against vendor/aeron's upstream test suite
	uv run scripts/upstream_tests.py

interop-smoke:  ## Run the Java<->Rust CnC layout interop smoke test
	INTEROP_SOAK_MESSAGES=1 uv run scripts/interop_smoke.py

interop-soak:  ## Run a bounded Java Aeron UDP soak through the Rust driver
	INTEROP_SOAK_MESSAGES=$(INTEROP_SOAK_MESSAGES) uv run scripts/interop_smoke.py

interop-rust-smoke:  ## Run Rust multi-process IPC and UDP smoke probes
	INTEROP_SOAK_MESSAGES=1 uv run scripts/rust_process_smoke.py

interop-rust-soak:  ## Run a bounded multi-process Rust UDP message soak
	INTEROP_SOAK_MESSAGES=$(INTEROP_SOAK_MESSAGES) uv run scripts/rust_process_smoke.py

cluster-failover-repro:  ## Run the real-process Tokio UDP leader-kill comparison
	uv run scripts/cluster_failover_repro.py

interop:  ## Run the full interop suite (not yet implemented)
	@echo "interop not yet implemented — see deploy/interop/"

status:  ## Show repo + submodule status
	@git status --short
	@git submodule status
