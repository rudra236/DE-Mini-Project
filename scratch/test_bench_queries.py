import time

import duckdb
import numpy as np
import pandas as pd
import polars as pl

n = 1_000_000
np.random.seed(42)

# Source data
txn_ids = np.arange(1, n + 1)
accounts = np.random.randint(10000, 15000, size=n)
amounts = np.round(np.random.lognormal(mean=7.0, sigma=1.0, size=n), 2)
currencies = np.random.choice(['INR', 'USD', 'EUR'], size=n, p=[0.9, 0.07, 0.03])
statuses = np.random.choice(['COMPLETED', 'PENDING'], size=n, p=[0.95, 0.05])

df_src = pd.DataFrame({
    "txn_id": txn_ids,
    "account_id": accounts,
    "amount": amounts,
    "currency": currencies,
    "status": statuses
})

# Faults
n_missing = 15_000
n_dup = 10_000
n_mismatch = 5_000

# Payment: drop first n_missing
df_pay = df_src.iloc[n_missing:].copy()

# Add duplicates
dup_rows = df_pay.iloc[:n_dup].copy()
df_pay = pd.concat([df_pay, dup_rows], ignore_index=True)

# Mismatch amounts
mismatch_idx = np.arange(n_dup, n_dup + n_mismatch)
df_pay.loc[mismatch_idx, "amount"] = np.round(df_pay.loc[mismatch_idx, "amount"] + 15.50, 2)

src_path = "scratch/src_test.parquet"
pay_path = "scratch/pay_test.parquet"
df_src.to_parquet(src_path, index=False)
df_pay.to_parquet(pay_path, index=False)

# 1. PANDAS
t0 = time.perf_counter()
p_src = pd.read_parquet(src_path)
p_pay = pd.read_parquet(pay_path)
p_missing = len(set(p_src["txn_id"]) - set(p_pay["txn_id"]))
p_dup = (p_pay["txn_id"].value_counts() > 1).sum()
# Inner join on unique IDs to compare amounts
p_inner = pd.merge(
    p_src[["txn_id", "amount"]].drop_duplicates("txn_id"),
    p_pay[["txn_id", "amount"]].drop_duplicates("txn_id"),
    on="txn_id",
    suffixes=("_s", "_p")
)
p_mismatch = (p_inner["amount_s"] != p_inner["amount_p"]).sum()
t_pandas = time.perf_counter() - t0
print(f"Pandas: missing={p_missing}, dup={p_dup}, mismatch={p_mismatch} in {t_pandas:.3f}s")

# 2. DUCKDB
t0 = time.perf_counter()
con = duckdb.connect()
d_missing = con.execute(f"""
    SELECT COUNT(*) FROM (
        SELECT txn_id FROM '{src_path}'
        EXCEPT
        SELECT txn_id FROM '{pay_path}'
    )
""").fetchone()[0]

d_dup = con.execute(f"""
    SELECT COUNT(*) FROM (
        SELECT txn_id FROM '{pay_path}' GROUP BY txn_id HAVING COUNT(*) > 1
    )
""").fetchone()[0]

d_mismatch = con.execute(f"""
    SELECT COUNT(*) FROM (
        SELECT DISTINCT txn_id, amount FROM '{src_path}'
    ) s
    JOIN (
        SELECT DISTINCT txn_id, amount FROM '{pay_path}'
    ) p ON s.txn_id = p.txn_id
    WHERE s.amount != p.amount
""").fetchone()[0]
t_duckdb = time.perf_counter() - t0
print(f"DuckDB: missing={d_missing}, dup={d_dup}, mismatch={d_mismatch} in {t_duckdb:.3f}s")

# 3. POLARS
t0 = time.perf_counter()
pl_src = pl.scan_parquet(src_path)
pl_pay = pl.scan_parquet(pay_path)

pl_missing = pl_src.select("txn_id").unique().join(
    pl_pay.select("txn_id").unique(),
    on="txn_id",
    how="anti"
).select(pl.len()).collect().item()

pl_dup = pl_pay.group_by("txn_id").len().filter(
    pl.col("len") > 1
).select(pl.len()).collect().item()

s_u = pl_src.select(["txn_id", "amount"]).unique("txn_id")
p_u = pl_pay.select(["txn_id", "amount"]).unique("txn_id")
pl_mismatch = s_u.join(p_u, on="txn_id").filter(
    pl.col("amount") != pl.col("amount_right")
).select(pl.len()).collect().item()
t_polars = time.perf_counter() - t0
print(f"Polars: missing={pl_missing}, dup={pl_dup}, mismatch={pl_mismatch} in {t_polars:.3f}s")

assert p_missing == d_missing == pl_missing
assert p_dup == d_dup == pl_dup
assert p_mismatch == d_mismatch == pl_mismatch
print("ALL THREE ENGINES PRODUCE IDENTICAL RESULTS!")
