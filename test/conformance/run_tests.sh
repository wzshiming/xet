#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "Building Rust conformance tools..."
cargo build --release

echo ""
echo "Running conformance tests..."
go test -v

echo ""
echo "All conformance tests passed!"
