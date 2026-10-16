import time

import numpy as np
import polars as pl

t0 = time.perf_counter()
n = 1_000_000

# Fast generation with NumPy
txn_ids = np.arange(1, n + 1)
accounts = np.random.randint(10000, 15000, size=n)
amounts = np.round(np.random.lognormal(mean=7.0, sigma=1.0, size=n), 2)
currencies = np.random.choice(['INR', 'USD', 'EUR'], size=n, p=[0.9, 0.07, 0.03])
statuses = np.random.choice(['COMPLETED', 'PENDING'], size=n, p=[0.95, 0.05])

df_source = pl.DataFrame({
    "txn_id": txn_ids,
    "account_id": accounts,
    "amount": amounts,
    "currency": currencies,
    "status": statuses
})

print(f"Generated 1M source in {time.perf_counter() - t0:.2f}s")
