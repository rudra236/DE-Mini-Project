# Utility & Verification Scripts (`scripts/`)

## How to Run
```bash
python scripts/check_stack.py
```

## Input
- Environment credentials from `.env` or system environment.
- Running Docker containers (`bankflow-postgres`, `bankflow-mongo`, `bankflow-kafka`).

## Output
- Verification status printout for each infrastructure service (PostgreSQL, MongoDB, Kafka).
