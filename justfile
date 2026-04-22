# QSLib development justfile

# Build the Python extension in development mode
build:
    uv run maturin develop --uv

# Run all tests (Rust + Python)
test: build test-rust test-python

# Run Rust tests only
test-rust:
    cargo test

# Run Python tests only (assumes already built)
test-python:
    uv run pytest

# Run all coverage recipes
coverage: coverage-rust coverage-rust-from-python coverage-python

# Lint Rust code
clippy:
    cargo clippy

# Lint Python code
ruff:
    uvx ruff check python/qslib

# Lint all code
lint: clippy ruff

# Format all code
fmt:
    cargo fmt
    uv run ruff format

# Check formatting without modifying
fmt-check:
    cargo fmt --check
    uv run ruff format --check

# Run mypy type checking
typecheck:
    uv run mypy --pretty --show-error-context --ignore-missing-imports python

# Run lint, format check, and tests
check: lint fmt-check test

# Run Rust benchmarks
bench:
    cargo bench

# Rust coverage from Rust tests
coverage-rust:
    #!/usr/bin/env bash
    set -euo pipefail
    source <(cargo llvm-cov show-env --export-prefix --no-cfg-coverage)
    export CARGO_TARGET_DIR=$CARGO_LLVM_COV_TARGET_DIR
    export CARGO_INCREMENTAL=1
    cargo llvm-cov clean --workspace
    cargo test
    cargo llvm-cov --no-run --lcov --output-path coverage-rust.lcov

# Rust coverage from Python tests (builds instrumented .so)
coverage-rust-from-python:
    #!/usr/bin/env bash
    set -euo pipefail
    source <(cargo llvm-cov show-env --export-prefix --no-cfg-coverage)
    export CARGO_TARGET_DIR=$CARGO_LLVM_COV_TARGET_DIR
    export CARGO_INCREMENTAL=1
    cargo llvm-cov clean --workspace
    if [ -f .venv/bin/activate ]; then
        source .venv/bin/activate
    else
        source .venv/Scripts/activate
    fi
    maturin develop --uv --profile dev
    pytest -k 'not test_real' --no-cov -x
    cargo llvm-cov report --lcov --output-path coverage-rust-from-python.lcov

# Python coverage via pytest-cov (no Rust instrumentation)
coverage-python:
    uv run pytest --cov qslib --cov-report term-missing --cov-report xml:coverage-python.xml

# Python + Rust coverage from Python tests (instrumented build, both outputs)
coverage-python-full:
    #!/usr/bin/env bash
    set -euo pipefail
    source <(cargo llvm-cov show-env --export-prefix --no-cfg-coverage)
    export CARGO_TARGET_DIR=$CARGO_LLVM_COV_TARGET_DIR
    export CARGO_INCREMENTAL=1
    cargo llvm-cov clean --workspace
    if [ -f .venv/bin/activate ]; then
        source .venv/bin/activate
    else
        source .venv/Scripts/activate
    fi
    maturin develop --uv --profile dev
    pytest --cov qslib --cov-report term-missing --cov-report xml:coverage-python.xml
    cargo llvm-cov report --lcov --output-path coverage-rust-from-python.lcov

# Rust integration tests with coverage (requires machine connection)
cov-rust-integration:
    #!/usr/bin/env bash
    set -euo pipefail
    source <(cargo llvm-cov show-env --export-prefix --no-cfg-coverage)
    export CARGO_TARGET_DIR=$CARGO_LLVM_COV_TARGET_DIR
    export CARGO_INCREMENTAL=1
    cargo llvm-cov clean --workspace
    cargo test --test real_integration -- --ignored
    cargo llvm-cov --no-run --lcov --output-path coverage-integration.lcov

# Test Python against pre-built wheels (used in CI after venv setup)
ci-test-python:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -f .venv/bin/activate ]; then
        source .venv/bin/activate
    else
        source .venv/Scripts/activate
    fi
    pytest -k 'not test_real' --cov qslib --cov-report xml --cov-report term

# Merge Rust lcov reports and generate HTML for local browsing
coverage-html: coverage-rust coverage-rust-from-python
    #!/usr/bin/env bash
    set -euo pipefail
    lcov -a coverage-rust.lcov -a coverage-rust-from-python.lcov -o coverage-rust-combined.lcov
    rm -rf htmlcov/rust
    genhtml coverage-rust-combined.lcov -o htmlcov/rust
    uv run pytest --cov qslib --cov-report html:htmlcov/python --cov-report term-missing --no-header

# Full coverage HTML including real machine integration tests (requires forwarding to machine)
coverage-html-integration:
    #!/usr/bin/env bash
    set -euo pipefail
    source <(cargo llvm-cov show-env --export-prefix --no-cfg-coverage)
    export CARGO_TARGET_DIR=$CARGO_LLVM_COV_TARGET_DIR
    export CARGO_INCREMENTAL=1
    export QSLIB_TEST_MACHINE=127.0.0.1
    export QSLIB_TEST_PORT=7000
    export QSLIB_TEST_SSL=false
    export QSLIB_TEST_PASSWORD=correctpassword
    cargo llvm-cov clean --workspace
    # Rust unit tests
    cargo test
    # Rust real integration tests
    cargo test --test real_integration -- --ignored --test-threads=4
    # Build instrumented Python extension
    .venv/bin/maturin develop --uv --profile dev
    # Python tests (all, including real-machine) — collects both Python and Rust coverage
    # Allow test failures so coverage reports are still generated
    .venv/bin/pytest --cov qslib --cov-report html:htmlcov/python --cov-report term-missing --no-header || PYTEST_FAILED=1
    # Rust coverage HTML from all collected profraw data
    rm -rf htmlcov/rust
    cargo llvm-cov report --html --output-dir htmlcov/rust
    if [ "${PYTEST_FAILED:-}" = "1" ]; then echo "Warning: some Python tests failed (see above)"; exit 1; fi

# Run real machine integration tests (Rust + Python)
# --test-threads=4: machine can't handle 30+ concurrent connections
integration:
    cargo test --test real_integration -- --ignored --test-threads=4
    uv run pytest tests/test_real.py

# Remove build artifacts and coverage files
clean:
    cargo clean
    rm -rf htmlcov/ .coverage coverage-*.lcov coverage-*.xml coverage-rust-combined.lcov
    find . -name '*.profraw' -delete
