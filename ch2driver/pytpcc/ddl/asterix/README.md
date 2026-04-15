# AsterixDB DDL for CH2++

DDL scripts use the **`.sqlpp`** extension (SQL++ for AsterixDB).

## Files

| File | Purpose |
|------|---------|
| [`ch2pp_bench.sqlpp`](ch2pp_bench.sqlpp) | **`DROP`/`CREATE DATAVERSE bench`**, ADM types, **`CREATE DATASET`**, secondary **`CREATE INDEX`**. Run this first. |
| [`ch2pp_load_example.sqlpp`](ch2pp_load_example.sqlpp) | Commented **`LOAD DATASET`** patterns + **`ANALYZE DATASET`**. Uncomment paths after JSON exists under `output_dir`. |
| [`ch2pp_schema_template.sqlpp`](ch2pp_schema_template.sqlpp) | Minimal placeholder (superseded by `ch2pp_bench.sqlpp` for full CH2++). |

## Workflow

1. Apply schema:  
   `python scripts/asterix/load_ddl.py --url http://127.0.0.1:19002/query/service --file ddl/asterix/ch2pp_bench.sqlpp`
2. Generate JSON: `python tpcc.py nestcollectionsdocgen --ch2pp --docgen-load ...` (or `tpcc.py asterix --docgen-load ...`).
3. Edit `ch2pp_load_example.sqlpp`: set `localfs` paths, uncomment `LOAD DATASET` lines, run `load_ddl.py` on that file.
4. Run benchmark: `python tpcc.py asterix --config examples/asterix.ini --ch2pp --no-load ...`

## Document counts

Same as `BENCHMARKING.md` (per `W` warehouses): warehouse `W`, district `10*W`, history `30000*W`, etc.

## Dialect note

Analytical queries under `analytical_queries/asterix/` are adapted from Couchbase N1QL. Validate on your AsterixDB version; adjust types in `ch2pp_bench.sqlpp` if loads fail (open types, datetime vs string).
