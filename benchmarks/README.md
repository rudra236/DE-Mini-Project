# Benchmarks Module (`benchmarks/`) — Experiment 8

## Overview
Evaluates reconciliation performance across three analytical processing engines on large-scale datasets (**1,000,000+ rows**):
1. **Pandas**: Standard in-memory DataFrame operations.
2. **DuckDB**: Vectorized SQL querying Parquet files directly.
3. **Polars**: High-performance multi-threaded DataFrame library using lazy query plans and Apache Arrow.

All benchmarks execute in **isolated subprocesses** with real-time RSS memory sampling via `psutil` to guarantee unpolluted measurements.

---

## How to Run
```bash
# Run 1M row benchmark (default)
python -m benchmarks.run_benchmark --rows 1000000

# Run 5M row benchmark
python -m benchmarks.run_benchmark --rows 5000000
```

---

## Output Artifacts
- **Markdown Documentation**: [`docs/benchmark.md`](../docs/benchmark.md)
- **Raw Data CSV**: [`benchmarks/results/results.csv`](results/results.csv)
- **Matplotlib Chart**: [`benchmarks/results/benchmark_comparison.png`](results/benchmark_comparison.png)
- **Interactive Plotly Chart**: [`benchmarks/results/benchmark_comparison.html`](results/benchmark_comparison.html)
