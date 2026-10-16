# Notebooks Module (`notebooks/`) — Experiment 1

## Overview
Contains exploratory data analysis and data cleansing notebooks for BankFlow Sentinel:
- [`01_eda_cleaning.ipynb`](01_eda_cleaning.ipynb): Comprehensive EDA covering schema inspection (`df.info()`), descriptive statistics (`describe()`), null counts, duplicate transaction IDs, invalid/negative amounts, IQR outlier analysis with boxplots/histograms, categorical distributions, hourly volume curves, and before/after cleaning metrics.

---

## How to Run

### Re-generate and Execute Programmatically
```bash
python scripts/build_eda_notebook.py
```

### Launch in Jupyter
```bash
jupyter notebook notebooks/01_eda_cleaning.ipynb
```
