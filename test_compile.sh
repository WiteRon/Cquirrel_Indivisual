#!/bin/bash

echo "=== Testing Compilation ==="
cd "$(dirname "$0")"

echo "[1/2] Cleaning..."
mvn clean -q
echo "  ✓ Clean completed"

echo "[2/2] Compiling..."
mvn compile -q
if [ $? -eq 0 ]; then
    echo "  ✓ Compilation successful!"
    echo ""
    echo "✅ Project compiles successfully with Java 8"
    exit 0
else
    echo "  ✗ Compilation failed!"
    exit 1
fi

