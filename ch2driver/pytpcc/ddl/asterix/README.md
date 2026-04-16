# AsterixDB DDL for CH2++

DDL scripts use the **`.sqlpp`** extension (SQL++ for AsterixDB).

## Files

| File | Purpose |
|------|---------|
| [`ch2pp_bench.sqlpp`](ch2pp_bench.sqlpp) | **`DROP`/`CREATE DATAVERSE bench`**, ADM types, **`CREATE DATASET`**, secondary **`CREATE INDEX`**. Run this first. |
| [`ch2pp_load_example.sqlpp`](ch2pp_load_example.sqlpp) | Minimal **illustrative** `LOAD DATASET` (comma-separated `host://` file URIs). For real loads, use **`scripts/asterix/generate_load_sqlpp.py`** from your docgen tree. |
| [`ch2pp_schema_template.sqlpp`](ch2pp_schema_template.sqlpp) | Minimal placeholder (superseded by `ch2pp_bench.sqlpp` for full CH2++). |

## Workflow

1. Apply schema (optional **`--dataverse NAME`** / **`-D`** on both tools so DDL matches `[asterix] dataverse`):  
   `python scripts/asterix/load_ddl.py --url http://127.0.0.1:19002/query/service --dataverse mydv --file ddl/asterix/ch2pp_bench.sqlpp`
2. Generate JSON: `python tpcc.py nestcollectionsdocgen --ch2pp --docgen-load ...` (or `tpcc.py asterix --docgen-load ...`). Lines match ADM fields only (no extra `key` property—PK columns are inside the JSON).
3. Generate bulk-load SQL from `output_dir` (all `output_dir/<dataset>/*.json` shards), then post to Asterix:  
   `python scripts/asterix/generate_load_sqlpp.py --output-dir /tmp/ch2_data --dataverse mydv --out /tmp/ch2_load.sqlpp`  
   `python scripts/asterix/load_ddl.py --url http://127.0.0.1:19002/query/service --dataverse mydv --file /tmp/ch2_load.sqlpp`  
   Match `--output-dir` and **`--dataverse`** to your INI. Use `--nc-host` if needed for `localfs`. For non-`bench` names in custom SQL, use **`load_ddl.py --dataverse-from OLD --dataverse NEW`**.
4. Run benchmark: `python tpcc.py asterix --config examples/asterix.ini --ch2pp --no-load ...`

## Document counts

Same as `BENCHMARKING.md` (per `W` warehouses): warehouse `W`, district `10*W`, history `30000*W`, etc.

## Dialect note

Analytical queries under `analytical_queries/asterix/` are adapted from Couchbase N1QL. Validate on your AsterixDB version; adjust types in `ch2pp_bench.sqlpp` if loads fail (open types, datetime vs string). Year extraction from string timestamps uses Asterix `get_year(datetime(o_entry_d))`, not N1QL `date_part_str`. Date arithmetic uses `datetime(...) + duration("PnD")` (e.g. `P7D` for one week), not N1QL `date_add_str`.


## Troubleshooting

- **`ASX1085` type already exists:** Often caused by **`load_ddl.py` posting each statement in a separate HTTP request**, so `USE` did not apply to `CREATE TYPE`. Current `load_ddl.py` prepends `USE` for in-dataverse statements. If you still see duplicates, run `DROP DATAVERSE ... IF EXISTS` (or drop the old dataverse in the UI) before re-applying schema.
