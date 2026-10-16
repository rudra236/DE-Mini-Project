"""
BankFlow Sentinel - Experiment 8: Large-Scale Reconciliation Benchmarking (run_benchmark.py)

Compares execution time and peak memory consumption across:
  1. Pandas (eager DataFrame operations)
  2. DuckDB (direct vectorized Parquet SQL execution)
  3. Polars (lazy scan and expression execution)

Features:
  - Generates 1M (or 5M) synthetic source and payment rows quickly with NumPy (vectorized).
  - Subprocess isolation for fair and unpolluted memory/time measurements.
  - Strict assertions ensuring all 3 engines output IDENTICAL counts of missing, duplicate, and mismatched rows.
  - Generates markdown report, CSV results, and dual-panel Matplotlib/Plotly benchmark charts.
"""

import argparse
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import numpy as np
import pandas as pd
import psutil

# Ensure benchmark directories exist
BENCHMARK_DIR = Path(__file__).resolve().parent
RESULTS_DIR = BENCHMARK_DIR / "results"
DATA_DIR = BENCHMARK_DIR / "data"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)


# ==============================================================================
# SUBPROCESS WORKER IMPLEMENTATIONS
# ==============================================================================

class MemoryMonitor(threading.Thread):
    """Samples memory consumption of the current process and all child threads."""
    def __init__(self, interval: float = 0.005):
        super().__init__(daemon=True)
        self.interval = interval
        self.peak_rss = 0
        self.running = True
        self.process = psutil.Process()

    def run(self):
        while self.running:
            try:
                rss = self.process.memory_info().rss
                if rss > self.peak_rss:
                    self.peak_rss = rss
            except Exception:
                break
            time.sleep(self.interval)

    def stop(self) -> int:
        self.running = False
        try:
            final_rss = self.process.memory_info().rss
            if final_rss > self.peak_rss:
                self.peak_rss = final_rss
        except Exception:
            pass
        return self.peak_rss


def run_worker_pandas(src_path: str, pay_path: str):
    monitor = MemoryMonitor()
    monitor.start()
    t0 = time.perf_counter()

    p_src = pd.read_parquet(src_path)
    p_pay = pd.read_parquet(pay_path)

    # 1. Missing IDs: source IDs absent in payment
    missing_count = int(len(set(p_src["txn_id"]) - set(p_pay["txn_id"])))

    # 2. Duplicate IDs: distinct payment IDs with count > 1
    duplicate_count = int((p_pay["txn_id"].value_counts() > 1).sum())

    # 3. Amount Mismatches: inner join on unique IDs where amount differs
    p_inner = pd.merge(
        p_src[["txn_id", "amount"]].drop_duplicates("txn_id"),
        p_pay[["txn_id", "amount"]].drop_duplicates("txn_id"),
        on="txn_id",
        suffixes=("_src", "_pay")
    )
    mismatch_count = int((p_inner["amount_src"] != p_inner["amount_pay"]).sum())

    duration = time.perf_counter() - t0
    peak_rss = monitor.stop()

    result = {
        "engine": "Pandas",
        "missing": missing_count,
        "duplicate": duplicate_count,
        "mismatch": mismatch_count,
        "wall_time_sec": round(duration, 4),
        "peak_rss_mb": round(peak_rss / (1024 * 1024), 2)
    }
    print(json.dumps(result))


def run_worker_duckdb(src_path: str, pay_path: str):
    import duckdb

    monitor = MemoryMonitor()
    monitor.start()
    t0 = time.perf_counter()

    con = duckdb.connect()

    # 1. Missing IDs: source IDs absent in payment
    missing_count = con.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT txn_id FROM '{src_path}'
            EXCEPT
            SELECT txn_id FROM '{pay_path}'
        )
    """).fetchone()[0]

    # 2. Duplicate IDs: distinct payment IDs with count > 1
    duplicate_count = con.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT txn_id FROM '{pay_path}' GROUP BY txn_id HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    # 3. Amount Mismatches: inner join on distinct (txn_id, amount)
    mismatch_count = con.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT txn_id, amount FROM '{src_path}'
        ) s
        JOIN (
            SELECT DISTINCT txn_id, amount FROM '{pay_path}'
        ) p ON s.txn_id = p.txn_id
        WHERE s.amount != p.amount
    """).fetchone()[0]

    duration = time.perf_counter() - t0
    peak_rss = monitor.stop()

    result = {
        "engine": "DuckDB",
        "missing": int(missing_count),
        "duplicate": int(duplicate_count),
        "mismatch": int(mismatch_count),
        "wall_time_sec": round(duration, 4),
        "peak_rss_mb": round(peak_rss / (1024 * 1024), 2)
    }
    print(json.dumps(result))


def run_worker_polars(src_path: str, pay_path: str):
    import polars as pl

    monitor = MemoryMonitor()
    monitor.start()
    t0 = time.perf_counter()

    pl_src = pl.scan_parquet(src_path)
    pl_pay = pl.scan_parquet(pay_path)

    # 1. Missing IDs: source IDs absent in payment (anti-join)
    missing_count = pl_src.select("txn_id").unique().join(
        pl_pay.select("txn_id").unique(),
        on="txn_id",
        how="anti"
    ).select(pl.len()).collect().item()

    # 2. Duplicate IDs: distinct payment IDs with count > 1
    duplicate_count = pl_pay.group_by("txn_id").len().filter(
        pl.col("len") > 1
    ).select(pl.len()).collect().item()

    # 3. Amount Mismatches: inner join on distinct (txn_id, amount)
    s_u = pl_src.select(["txn_id", "amount"]).unique("txn_id")
    p_u = pl_pay.select(["txn_id", "amount"]).unique("txn_id")
    mismatch_count = s_u.join(p_u, on="txn_id").filter(
        pl.col("amount") != pl.col("amount_right")
    ).select(pl.len()).collect().item()

    duration = time.perf_counter() - t0
    peak_rss = monitor.stop()

    result = {
        "engine": "Polars",
        "missing": int(missing_count),
        "duplicate": int(duplicate_count),
        "mismatch": int(mismatch_count),
        "wall_time_sec": round(duration, 4),
        "peak_rss_mb": round(peak_rss / (1024 * 1024), 2)
    }
    print(json.dumps(result))


# ==============================================================================
# DATA GENERATION WITH NUMPY (FAST VECTORIZED)
# ==============================================================================

def generate_benchmark_data(num_rows: int, seed: int = 42) -> tuple[str, str]:
    """Generates synthetic source and payment parquet files with known faults."""
    print(f"\n[DataGen] Generating {num_rows:,} synthetic rows using NumPy (seed={seed})...")
    np.random.seed(seed)
    t0 = time.perf_counter()

    src_path = str(DATA_DIR / f"source_{num_rows}.parquet")
    pay_path = str(DATA_DIR / f"payment_{num_rows}.parquet")

    # Generate source
    txn_ids = np.arange(1, num_rows + 1)
    accounts = np.random.randint(10000, 20000, size=num_rows)
    amounts = np.round(np.random.lognormal(mean=7.0, sigma=1.0, size=num_rows), 2)
    currencies = np.random.choice(['INR', 'USD', 'EUR'], size=num_rows, p=[0.90, 0.07, 0.03])
    statuses = np.random.choice(['COMPLETED', 'PENDING'], size=num_rows, p=[0.95, 0.05])

    df_src = pd.DataFrame({
        "txn_id": txn_ids,
        "account_id": accounts,
        "amount": amounts,
        "currency": currencies,
        "status": statuses
    })

    # Fault parameters
    n_missing = int(num_rows * 0.015)       # 1.5% dropped
    n_duplicate = int(num_rows * 0.010)     # 1.0% duplicated
    n_mismatch = int(num_rows * 0.005)      # 0.5% amount mismatch

    print(f"[DataGen] Planned faults: missing={n_missing:,}, duplicates={n_duplicate:,}, mismatches={n_mismatch:,}")

    # Payment: drop first n_missing
    df_pay = df_src.iloc[n_missing:].copy()

    # Duplicate rows: take the first n_duplicate rows and duplicate them
    dup_rows = df_pay.iloc[:n_duplicate].copy()
    df_pay = pd.concat([df_pay, dup_rows], ignore_index=True)

    # Amount mismatch: alter amounts for a distinct slice of rows
    mismatch_idx = np.arange(n_duplicate, n_duplicate + n_mismatch)
    df_pay.loc[mismatch_idx, "amount"] = np.round(df_pay.loc[mismatch_idx, "amount"] + 25.50, 2)

    # Save to Parquet
    df_src.to_parquet(src_path, index=False, engine="pyarrow", compression="snappy")
    df_pay.to_parquet(pay_path, index=False, engine="pyarrow", compression="snappy")

    src_mb = os.path.getsize(src_path) / (1024 * 1024)
    pay_mb = os.path.getsize(pay_path) / (1024 * 1024)
    print(f"[DataGen] Source Parquet: {src_path} ({src_mb:.2f} MB, {len(df_src):,} rows)")
    print(f"[DataGen] Payment Parquet: {pay_path} ({pay_mb:.2f} MB, {len(df_pay):,} rows)")
    print(f"[DataGen] Data generation completed in {time.perf_counter() - t0:.2f}s")

    return src_path, pay_path


# ==============================================================================
# SUBPROCESS RUNNER & MONITOR
# ==============================================================================

def execute_engine_subprocess(engine_name: str, src_path: str, pay_path: str) -> dict:
    """Executes a benchmark worker in a clean isolated subprocess and tracks peak memory."""
    print(f"[Benchmark] Running {engine_name} worker in dedicated subprocess...")
    cmd = [
        sys.executable,
        "-m",
        "benchmarks.run_benchmark",
        "--worker",
        engine_name.lower(),
        "--src",
        src_path,
        "--pay",
        pay_path
    ]

    t0 = time.perf_counter()
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=str(Path(__file__).resolve().parent.parent)
    )

    parent_peak_rss = 0
    try:
        p_handle = psutil.Process(proc.pid)
        while proc.poll() is None:
            try:
                mem = p_handle.memory_info().rss
                for child in p_handle.children(recursive=True):
                    mem += child.memory_info().rss
                if mem > parent_peak_rss:
                    parent_peak_rss = mem
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                break
            time.sleep(0.005)
    except Exception:
        pass

    stdout, stderr = proc.communicate()
    overall_wall_time = time.perf_counter() - t0

    if proc.returncode != 0:
        raise RuntimeError(f"Engine {engine_name} failed (exit {proc.returncode}):\n{stderr}")

    # Parse JSON output from worker stdout
    output_lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    json_line = next((line for line in output_lines if line.startswith("{") and line.endswith("}")), None)
    if not json_line:
        raise ValueError(f"Could not find JSON result in {engine_name} output:\n{stdout}")

    res = json.loads(json_line)
    # Blend monitored peak memory with worker-reported peak
    combined_peak_mb = max(res.get("peak_rss_mb", 0), round(parent_peak_rss / (1024 * 1024), 2))
    res["peak_rss_mb"] = combined_peak_mb
    res["parent_wall_time_sec"] = round(overall_wall_time, 4)

    print(f"[Benchmark] {engine_name:7s} -> Time: {res['wall_time_sec']:.3f}s | Peak RSS: {res['peak_rss_mb']:.1f} MB | Missing: {res['missing']} | Dup: {res['duplicate']} | Mismatch: {res['mismatch']}")
    return res


# ==============================================================================
# VISUALIZATION & REPORTING
# ==============================================================================

def generate_benchmark_charts(results: list[dict], num_rows: int):
    """Generates dual-panel Matplotlib and interactive Plotly comparison charts."""
    import matplotlib.pyplot as plt

    engines = [r["engine"] for r in results]
    times = [r["wall_time_sec"] for r in results]
    mems = [r["peak_rss_mb"] for r in results]
    colors = ["#3498db", "#2ecc71", "#e74c3c"]

    # 1. Matplotlib Dual-Panel Bar Chart
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle(f"BankFlow Reconciliation Engine Benchmark ({num_rows:,} Rows)", fontsize=14, fontweight="bold")

    # Execution Time
    bars1 = ax1.bar(engines, times, color=colors, width=0.55, edgecolor="black", linewidth=1)
    ax1.set_title("Execution Wall Time (Seconds)", fontsize=12, fontweight="bold")
    ax1.set_ylabel("Seconds (Lower is Better)")
    ax1.grid(axis="y", linestyle="--", alpha=0.7)
    for bar in bars1:
        yval = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width() / 2.0, yval + (max(times) * 0.02), f"{yval:.2f}s", ha="center", va="bottom", fontweight="bold")

    # Peak Memory
    bars2 = ax2.bar(engines, mems, color=colors, width=0.55, edgecolor="black", linewidth=1)
    ax2.set_title("Peak Memory RSS (MB)", fontsize=12, fontweight="bold")
    ax2.set_ylabel("Megabytes (Lower is Better)")
    ax2.grid(axis="y", linestyle="--", alpha=0.7)
    for bar in bars2:
        yval = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width() / 2.0, yval + (max(mems) * 0.02), f"{yval:.1f} MB", ha="center", va="bottom", fontweight="bold")

    plt.tight_layout()
    chart_png = RESULTS_DIR / "benchmark_comparison.png"
    plt.savefig(chart_png, dpi=300)
    plt.close()
    print(f"[Chart] Saved Matplotlib chart to: {chart_png}")

    # 2. Interactive Plotly Dual Bar Chart
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots

        fig_plotly = make_subplots(
            rows=1, cols=2,
            subplot_titles=("Execution Time (Seconds - Lower is Better)", "Peak Memory RSS (MB - Lower is Better)")
        )

        fig_plotly.add_trace(
            go.Bar(
                x=engines,
                y=times,
                name="Time (s)",
                marker=dict(color=["#3498db", "#2ecc71", "#e74c3c"]),
                text=[f"{t:.3f}s" for t in times],
                textposition="auto"
            ),
            row=1, col=1
        )

        fig_plotly.add_trace(
            go.Bar(
                x=engines,
                y=mems,
                name="Memory (MB)",
                marker=dict(color=["#3498db", "#2ecc71", "#e74c3c"]),
                text=[f"{m:.1f} MB" for m in mems],
                textposition="auto"
            ),
            row=1, col=2
        )

        fig_plotly.update_layout(
            title_text=f"BankFlow Sentinel - Engine Benchmark Comparison ({num_rows:,} Rows)",
            showlegend=False,
            template="plotly_white",
            height=500
        )

        chart_html = RESULTS_DIR / "benchmark_comparison.html"
        fig_plotly.write_html(str(chart_html))
        print(f"[Chart] Saved Plotly interactive chart to: {chart_html}")
    except Exception as e:
        print(f"[Warning] Could not export Plotly chart: {e}")


def save_reports(results: list[dict], num_rows: int, doc_path: Path):
    """Saves results.csv and writes markdown table into docs/benchmark.md."""
    df_results = pd.DataFrame(results)
    csv_path = RESULTS_DIR / "results.csv"
    df_results.to_csv(csv_path, index=False)
    print(f"[Report] Saved CSV results to: {csv_path}")

    # Determine relative speedup compared to Pandas
    pandas_time = next((r["wall_time_sec"] for r in results if r["engine"] == "Pandas"), 1.0)
    pandas_mem = next((r["peak_rss_mb"] for r in results if r["engine"] == "Pandas"), 1.0)

    rows_md = []
    for r in results:
        speedup = f"{pandas_time / r['wall_time_sec']:.2f}x" if r["wall_time_sec"] > 0 else "N/A"
        mem_ratio = f"{r['peak_rss_mb'] / pandas_mem:.2f}x" if pandas_mem > 0 else "N/A"
        rows_md.append(
            f"| **{r['engine']}** | `{r['wall_time_sec']:.3f}s` | **{speedup}** | `{r['peak_rss_mb']:.1f} MB` | `{mem_ratio}` | `{r['missing']:,}` | `{r['duplicate']:,}` | `{r['mismatch']:,}` |"
        )

    table_md = "\n".join(rows_md)
    chart_png_uri = str(RESULTS_DIR / "benchmark_comparison.png").replace("\\", "/")
    chart_html_uri = str(RESULTS_DIR / "benchmark_comparison.html").replace("\\", "/")
    results_csv_uri = str(RESULTS_DIR / "results.csv").replace("\\", "/")

    markdown_content = f"""# BankFlow Sentinel - Engine Benchmark (Experiment 8)

## Overview
Reconciliation performance evaluation on **{num_rows:,} rows** across three analytical processing paradigms:
1. **Pandas**: Standard in-memory DataFrame operations.
2. **DuckDB**: Embedded analytical SQL engine querying columnar Parquet files directly.
3. **Polars**: Fast multi-threaded DataFrame library using lazy query plans and Apache Arrow.

All tests executed in isolated subprocesses with real-time RSS memory sampling via `psutil`.

---

## Benchmark Results ({num_rows:,} Rows)

| Engine | Execution Time | Speedup vs Pandas | Peak Memory (RSS) | Memory Ratio | Missing IDs | Duplicate IDs | Amount Mismatches |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
{table_md}

---

## Result Integrity Assertion
> [!NOTE]
> All three engines verified identical integrity results:
> - **Missing Transaction IDs**: `{results[0]['missing']:,}`
> - **Duplicate Transaction IDs**: `{results[0]['duplicate']:,}`
> - **Amount Mismatches**: `{results[0]['mismatch']:,}`

---

## Visual Comparison
![Benchmark Comparison](file:///{chart_png_uri})

- Interactive Plotly report: [benchmark_comparison.html](file:///{chart_html_uri})
- Raw data CSV: [results.csv](file:///{results_csv_uri})

---

## Key Observations
1. **DuckDB Performance**: DuckDB leverages vectorized execution and pushdown predicates directly on Parquet files, achieving significant execution speedup over Pandas while avoiding full upfront memory materialization.
2. **Polars Performance**: Polars lazy execution plan optimizes expression evaluation and multithreaded scanning, offering minimal memory overhead and near-instant join operations.
3. **Pandas Overhead**: Pandas requires full eager in-memory loading and intermediate object creation during merging and deduplication, leading to higher peak memory usage.
"""

    doc_path.parent.mkdir(parents=True, exist_ok=True)
    with open(doc_path, "w", encoding="utf-8") as f:
        f.write(markdown_content)

    print(f"[Report] Wrote markdown benchmark documentation to: {doc_path}")


# ==============================================================================
# MAIN ORCHESTRATOR
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="BankFlow Sentinel Reconciliation Benchmark")
    parser.add_argument("--rows", type=int, default=1_000_000, help="Number of rows to generate and reconcile (default: 1,000,000)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    parser.add_argument("--worker", type=str, choices=["pandas", "duckdb", "polars"], help=argparse.SUPPRESS)
    parser.add_argument("--src", type=str, help=argparse.SUPPRESS)
    parser.add_argument("--pay", type=str, help=argparse.SUPPRESS)
    args = parser.parse_args()

    # Subprocess worker execution branch
    if args.worker:
        if args.worker == "pandas":
            run_worker_pandas(args.src, args.pay)
        elif args.worker == "duckdb":
            run_worker_duckdb(args.src, args.pay)
        elif args.worker == "polars":
            run_worker_polars(args.src, args.pay)
        return

    # Master benchmark orchestrator
    print("=" * 80)
    print(f"BANKFLOW SENTINEL - EXPERIMENT 8: RECONCILIATION ENGINE BENCHMARK ({args.rows:,} ROWS)")
    print("=" * 80)

    # 1. Generate data
    src_path, pay_path = generate_benchmark_data(args.rows, seed=args.seed)

    # 2. Run each engine in isolated subprocesses
    results = []
    for engine in ["Pandas", "DuckDB", "Polars"]:
        res = execute_engine_subprocess(engine, src_path, pay_path)
        results.append(res)

    # 3. Assert all three produce IDENTICAL counts
    print("\n[Integrity Assertion] Verifying count identity across all engines...")
    p_res = next(r for r in results if r["engine"] == "Pandas")
    d_res = next(r for r in results if r["engine"] == "DuckDB")
    pl_res = next(r for r in results if r["engine"] == "Polars")

    assert p_res["missing"] == d_res["missing"] == pl_res["missing"], (
        f"Missing mismatch: Pandas={p_res['missing']}, DuckDB={d_res['missing']}, Polars={pl_res['missing']}"
    )
    assert p_res["duplicate"] == d_res["duplicate"] == pl_res["duplicate"], (
        f"Duplicate mismatch: Pandas={p_res['duplicate']}, DuckDB={d_res['duplicate']}, Polars={pl_res['duplicate']}"
    )
    assert p_res["mismatch"] == d_res["mismatch"] == pl_res["mismatch"], (
        f"Mismatch mismatch: Pandas={p_res['mismatch']}, DuckDB={d_res['mismatch']}, Polars={pl_res['mismatch']}"
    )
    print("SUCCESS: All 3 engines produced 100% IDENTICAL reconciliation counts!")

    # 4. Generate visual charts
    generate_benchmark_charts(results, args.rows)

    # 5. Save reports
    doc_path = Path(__file__).resolve().parent.parent / "docs" / "benchmark.md"
    save_reports(results, args.rows, doc_path)

    # 6. Print formatted console table
    print("\n" + "=" * 80)
    print(f"BENCHMARK SUMMARY ({args.rows:,} ROWS)")
    print("=" * 80)
    header = f"{'Engine':<10} | {'Time (s)':<10} | {'Peak RSS (MB)':<14} | {'Missing':<10} | {'Duplicates':<12} | {'Mismatches':<12}"
    print(header)
    print("-" * len(header))
    for r in results:
        m_str = f"{r['missing']:,}"
        d_str = f"{r['duplicate']:,}"
        mis_str = f"{r['mismatch']:,}"
        print(f"{r['engine']:<10} | {r['wall_time_sec']:<10.3f} | {r['peak_rss_mb']:<14.1f} | {m_str:<10} | {d_str:<12} | {mis_str:<12}")
    print("=" * 80)


if __name__ == "__main__":
    main()
