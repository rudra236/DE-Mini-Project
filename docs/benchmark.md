# BankFlow Sentinel - Engine Benchmark (Experiment 8)

## Overview
Reconciliation performance evaluation on **1,000,000 rows** across three analytical processing paradigms:
1. **Pandas**: Standard in-memory DataFrame operations.
2. **DuckDB**: Embedded analytical SQL engine querying columnar Parquet files directly.
3. **Polars**: Fast multi-threaded DataFrame library using lazy query plans and Apache Arrow.

All tests executed in isolated subprocesses with real-time RSS memory sampling via `psutil`.

---

## Benchmark Results (1,000,000 Rows)

| Engine | Execution Time | Speedup vs Pandas | Peak Memory (RSS) | Memory Ratio | Missing IDs | Duplicate IDs | Amount Mismatches |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Pandas** | `0.664s` | **1.00x** | `350.6 MB` | `1.00x` | `15,000` | `10,000` | `5,000` |
| **DuckDB** | `0.297s` | **2.24x** | `170.7 MB` | `0.49x` | `15,000` | `10,000` | `5,000` |
| **Polars** | `0.336s` | **1.98x** | `424.5 MB` | `1.21x` | `15,000` | `10,000` | `5,000` |

---

## Result Integrity Assertion
> [!NOTE]
> All three engines verified identical integrity results:
> - **Missing Transaction IDs**: `15,000`
> - **Duplicate Transaction IDs**: `10,000`
> - **Amount Mismatches**: `5,000`

---

## Visual Comparison
![Benchmark Comparison](file:///C:/Users/Rudra/Desktop/de mini project/benchmarks/results/benchmark_comparison.png)

- Interactive Plotly report: [benchmark_comparison.html](file:///C:/Users/Rudra/Desktop/de mini project/benchmarks/results/benchmark_comparison.html)
- Raw data CSV: [results.csv](file:///C:/Users/Rudra/Desktop/de mini project/benchmarks/results/results.csv)

---

## Key Observations
1. **DuckDB Performance**: DuckDB leverages vectorized execution and pushdown predicates directly on Parquet files, achieving significant execution speedup over Pandas while avoiding full upfront memory materialization.
2. **Polars Performance**: Polars lazy execution plan optimizes expression evaluation and multithreaded scanning, offering minimal memory overhead and near-instant join operations.
3. **Pandas Overhead**: Pandas requires full eager in-memory loading and intermediate object creation during merging and deduplication, leading to higher peak memory usage.
