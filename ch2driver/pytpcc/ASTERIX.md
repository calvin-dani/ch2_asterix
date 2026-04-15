# Running CH2++ with Apache AsterixDB

This driver (`asterix`) runs the same harness as Couchbase: data generation, optional JSON file output, load, transactional TPC-C mix, and 22 analytical queries.

## Prerequisites

- Python 3.9+ with dependencies from the repo root [`requirements.txt`](../../requirements.txt). Recommended:

```bash
python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
```

- `requests` is optional for `AsterixDriver` (stdlib `urllib` is used by `scripts/asterix/load_ddl.py`).
- An AsterixDB cluster with HTTP API on the Cluster Controller (default port `19002`).
- DDL and datasets compatible with **CH2++** JSON (nested `c_name`, `o_orderline`, `s_dists`, etc.). See `ddl/asterix/README.md` and `ddl/asterix/ch2pp_schema_template.sqlpp`.

## Configuration

Create a config file passed with `tpcc.py --config`. A starting point is [`examples/asterix.ini`](./examples/asterix.ini). Example:

```ini
[asterix]
cc_host = 127.0.0.1
cc_port = 19002
use_tls = false
dataverse = bench
analytical_query_subdir = asterix
statement_timeout_sec = 3600
output_dir = /tmp/ch2_data
join_hint =
```

- **`dataverse`**: Must match the dataverse used in your DDL (`USE dataverse`).
- **`analytical_query_subdir`**: Subfolder under `analytical_queries/` (defaults to `asterix`).
- **`output_dir`**: Used with `--docgen-load` (JSON files per collection).

## Load modes

| Flag | Behavior |
|------|----------|
| `--docgen-load` | Write CH2++ JSON batches to `output_dir` (same layout as `nestcollectionsdocgen`). Then run `LOAD DATASET` via `scripts/asterix/load_ddl.py` or your own DDL. |
| `--asterix-http-insert` | Insert each generated document with `INSERT INTO dataset ([...])` over HTTP (slow; requires types that match JSON). |

## Example commands

**1. Generate JSON only (no DB):**

```bash
python tpcc.py nestcollectionsdocgen --ch2pp --warehouses 1 --tclients 1 --no-execute --docgen-load
```

**2. Load JSON into Asterix** (after editing paths in DDL):

```bash
python scripts/asterix/load_ddl.py --file ddl/asterix/your_schema.sqlpp --url http://127.0.0.1:19002/query/service
```

**3. Run benchmark (data already loaded):**

```bash
python tpcc.py asterix --config asterix.ini --ch2pp --no-load \
  --tclients 4 --aclients 1 --query-iterations 2 --warmup-query-iterations 1
```

**4. End-to-end load + run (HTTP insert):**

```bash
python tpcc.py asterix --config asterix.ini --ch2pp --asterix-http-insert \
  --warehouses 1 --tclients 4 --no-execute
# then run without load
python tpcc.py asterix --config asterix.ini --ch2pp --no-load --tclients 4 --aclients 1 \
  --query-iterations 2 --warmup-query-iterations 1
```

## Analytical queries

Files live in `analytical_queries/asterix/` (`ch2pp.sql` and optional `ch2pp_non_optimized.sql`). They are derived from the Couchbase N1QL versions with mechanical edits (modulo operator, stripped index hints). **Validate** on your AsterixDB version before relying on results.

## Environment

- `IGNORE_SKIP_INDEX_HINTS=1` — strip `skip-index` hints if any remain (same idea as Couchbase driver).
- `ACLIENT_REQUEST_PARAMS` — optional JSON merged into POST fields for `/query/service` (advanced).

## Limitations

- Transactional path uses **sequential** statements (no Couchbase-style `BEGIN`/`txid`). Semantics depend on AsterixDB transaction support for your deployment.
- `array_append` and nested `UPDATE` syntax must match your server version; adjust `drivers/asterixdriver.py` if the engine rejects a statement.
