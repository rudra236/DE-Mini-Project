from pathlib import Path

import nbformat as nbf
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent


def test_benchmark_results_and_markdown():
    csv_path = ROOT / "benchmarks" / "results" / "results.csv"
    doc_path = ROOT / "docs" / "benchmark.md"
    chart_png = ROOT / "benchmarks" / "results" / "benchmark_comparison.png"
    chart_html = ROOT / "benchmarks" / "results" / "benchmark_comparison.html"

    assert csv_path.exists(), f"results.csv missing at {csv_path}"
    assert doc_path.exists(), f"benchmark.md missing at {doc_path}"
    assert chart_png.exists(), f"benchmark_comparison.png missing at {chart_png}"
    assert chart_html.exists(), f"benchmark_comparison.html missing at {chart_html}"

    df = pd.read_csv(csv_path)
    assert len(df) == 3, f"Expected 3 engines in results.csv, found {len(df)}"
    engines = set(df["engine"].tolist())
    assert engines == {"Pandas", "DuckDB", "Polars"}, f"Unexpected engines: {engines}"

    # Verify identical counts across all engines
    assert len(df["missing"].unique()) == 1, "Missing counts differ across engines"
    assert len(df["duplicate"].unique()) == 1, "Duplicate counts differ across engines"
    assert len(df["mismatch"].unique()) == 1, "Mismatch counts differ across engines"


def test_eda_notebook_executed_with_outputs():
    nb_path = ROOT / "notebooks" / "01_eda_cleaning.ipynb"
    assert nb_path.exists(), f"Notebook missing at {nb_path}"

    nb = nbf.read(str(nb_path), as_version=4)
    code_cells = [c for c in nb.cells if c.cell_type == "code"]
    assert len(code_cells) >= 10, f"Expected >= 10 code cells, got {len(code_cells)}"

    # Assert every code cell has executed outputs
    for idx, cell in enumerate(code_cells, start=1):
        assert len(cell.outputs) > 0, f"Code cell {idx} has empty outputs"
