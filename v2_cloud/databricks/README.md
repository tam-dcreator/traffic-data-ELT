# V2 — Databricks / Spark Processing

This directory holds the Databricks/Spark processing layer for V2. It is a
placeholder for the **next** milestone and is intentionally not implemented yet.

## Responsibility boundary

```text
Databricks / Spark
→ decompression, distributed parsing, and heavy transformation
```

Spark reads pNEUMA source objects from **S3 Bronze**, parses them using the
shared Python parser, and writes **S3 Silver** as Parquet.

## Next milestone

```text
one extracted pNEUMA CSV
    ↓
shared Python parser  (traffic_data_elt.extract.PneumaExtractor.extract_from_lines)
    ↓
Spark DataFrame
    ↓
S3 Silver Parquet
```

### Reuse the shared parser

The core pNEUMA source-format logic (logical-record reconstruction, boundary
repair, frame validation) lives in the shared package and must not be
duplicated here:

```python
from traffic_data_elt.extract import PneumaExtractor

# lines: an iterable of decoded text lines from one Bronze CSV object
records = PneumaExtractor.extract_from_lines(source_file, lines)
```

`extract_from_lines` is source-agnostic — it accepts any iterable of decoded
text lines (local file, S3 object stream, or a Spark partition of lines), so
the same source-format intelligence powers both V1 (PostgreSQL) and V2 (Spark).

No Spark-specific code belongs in the core parser.

## Not in scope for the current change

- Gold layer
- Neon PostgreSQL serving warehouse
- dbt V2 models
- The full V2 Airflow orchestration DAG

These arrive in later milestones.
