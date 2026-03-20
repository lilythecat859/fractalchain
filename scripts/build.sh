# fractalchain/scripts/build.sh
#!/bin/bash

set -euo pipefail

# FRACTALCHAIN Build Script
# Usage: ./build.sh [target] [features]

TARGET=${1:-release}
FEATURES=${2:-default}

echo "🔨 Building FRACTALCHAIN..."
echo "Target: $TARGET"
echo "Features: $FEATURES"

# Set build flags
export RUSTFLAGS="-C target-cpu=native -C opt-level=3"
export CARGO_INCREMENTAL=0

# Build based on target
case "$TARGET" in
    debug)
        echo "Building debug version..."
        cargo build --features "$FEATURES"
        ;;
    release)
        echo "Building release version..."
        cargo build --release --features "$FEATURES"
        ;;
    bench)
        echo "Building benchmarks..."
        cargo bench --no-run --features "$FEATURES"
        ;;
    test)
        echo "Building tests..."
        cargo test --no-run --features "$FEATURES"
        ;;
    docker)
        echo "Building Docker image..."
        docker build -t fractalchain:latest .
        ;;
    *)
        echo "Unknown target: $TARGET"
        echo "Available targets: debug, release, bench, test, docker"
        exit 1
        ;;
esac

echo "✅ Build completed successfully!"

# Run tests if building release
if [ "$TARGET" = "release" ]; then
    echo "🧪 Running tests..."
    cargo test --release --features "$FEATURES"
fi

# Generate documentation if requested
if [ "${3:-}" = "docs" ]; then
    echo "📚 Generating documentation..."
    cargo doc --all --no-deps --features "$FEATURES"
fi