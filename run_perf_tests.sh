#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "=== Running Performance Tests ==="
dotnet test BddE2eTests/BddE2eTests.csproj \
    --filter "FullyQualifiedName~PerformanceMeasurement" \
    --no-build \
    -v n

echo ""
echo "=== Tests Complete ==="
echo ""

# Find the latest report directory
REPORT_DIR=$(ls -td BddE2eTests/perf_reports/*/ 2>/dev/null | head -1)

if [ -n "$REPORT_DIR" ]; then
    echo "Latest report: $REPORT_DIR"
    echo ""
    echo "=== Plotting Results ==="
    cd BddE2eTests
    pip install -q pandas matplotlib 2>/dev/null || pip3 install -q pandas matplotlib 2>/dev/null || true
    python3 plot_results.py "../$REPORT_DIR" || python plot_results.py "../$REPORT_DIR"
else
    echo "No reports found in BddE2eTests/perf_reports/"
fi
