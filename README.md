# mkpipe-loader-dynamodb

DynamoDB loader plugin for [MkPipe](https://github.com/mkpipe-etl/mkpipe). Writes Spark DataFrames into DynamoDB tables using `boto3` `batch_writer`.

## Documentation

For more detailed documentation, please visit the [GitHub repository](https://github.com/mkpipe-etl/mkpipe).

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

---

## Connection Configuration

```yaml
connections:
  dynamodb_target:
    variant: dynamodb
    region: us-east-1
    aws_access_key: AKIAIOSFODNN7EXAMPLE
    aws_secret_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

---

## Table Configuration

```yaml
pipelines:
  - name: pg_to_dynamodb
    source: pg_source
    destination: dynamodb_target
    tables:
      - name: public.events
        target_name: MyEventsTable
        replication_method: full
```

- `full` replication: deletes all existing items before writing (scans table keys first, then batch-deletes).
- `incremental` replication: appends / upserts items.

> **Note:** The destination table must exist in DynamoDB before loading. MkPipe does not create tables.

---

## Write Throughput

Write throughput is controlled by `batchsize` — used to chunk the `collect()`-ed rows before handing to `batch_writer`:

```yaml
      - name: public.events
        target_name: MyEventsTable
        replication_method: full
        batchsize: 10000
```

### Performance Notes

- DynamoDB `batch_writer` handles 25-item batches internally; `batchsize` controls the Python-level chunking before the writer, not the DynamoDB batch size.
- Write speed is ultimately limited by DynamoDB Write Capacity Units (WCUs). For on-demand tables this scales automatically; for provisioned tables set WCUs accordingly.
- DynamoDB loader uses `df.collect()` on the driver — not recommended for extremely large datasets (>10M rows). Consider splitting into smaller incremental loads.

---

## All Table Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Source table name |
| `target_name` | string | required | DynamoDB destination table name |
| `replication_method` | `full` / `incremental` | `full` | `full` clears table first; `incremental` upserts |
| `batchsize` | int | `10000` | Python-level chunk size before `batch_writer` |
| `dedup_columns` | list | — | Columns used for `mkpipe_id` hash deduplication |
| `tags` | list | `[]` | Tags for selective pipeline execution |
| `pass_on_error` | bool | `false` | Skip table on error instead of failing |
