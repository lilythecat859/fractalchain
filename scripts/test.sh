# fractalchain/scripts/test.sh
#!/bin/bash

set -euo pipefail

# FRACTALCHAIN Test Script
# Usage: ./test.sh [test_type] [features]

TEST_TYPE=${1:-all}
FEATURES=${2:-default}

echo "🧪 Running FRACTALCHAIN tests..."
echo "Test type: $TEST_TYPE"
echo "Features: $FEATURES"

# Set test flags
export RUST_BACKTRACE=1
export RUST_LOG=debug

# Run tests based on type
case "$TEST_TYPE" in
    all)
        echo "Running all tests..."
        cargo test --all --features "$FEATURES" -- --nocapture
        ;;
    unit)
        echo "Running unit tests..."
        cargo test --lib --features "$FEATURES" -- --nocapture
        ;;
    integration)
        echo "Running integration tests..."
        cargo test --test integration_tests --features "$FEATURES" -- --nocapture
        ;;
    property)
        echo "Running property tests..."
        cargo test --test property_tests --features "$FEATURES" -- --nocapture
        ;;
    bench)
        echo "Running benchmarks..."
        cargo bench --features "$FEATURES"
        ;;
    10m-tps)
        echo "Running 10M TPS test..."
        cargo test --test integration_tests test_10m_tps_target --release --features "$FEATURES" -- --nocapture
        ;;
    coverage)
        echo "Generating coverage report..."
        cargo tarpaulin --out Html --output-dir coverage/ --features "$FEATURES"
        ;;
    *)
        echo "Unknown test type: $TEST_TYPE"
        echo "Available types: all, unit, integration, property, bench, 10m-tps, coverage"
        exit 1
        ;;
esac

echo "✅ Tests completed successfully!"