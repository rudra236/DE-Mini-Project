# Kafka Streaming Module (`streaming/`)

## How to Run
Producer:
```bash
python -m streaming.producer --batch-id B001
```

Consumer:
```bash
python -m streaming.consumer
```

## Input
- Kafka events on topic `batch_created`:
  `{"batch_id":"B001","file_path":"data/batch_001","row_count":10000}`

## Output
- Consumer triggers downstream ETL and reconciliation workflows, logging each pipeline hop.
