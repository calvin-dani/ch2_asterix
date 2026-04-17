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

Defaults match [`AsterixDriver` `makeDefaultConfig`](./drivers/asterixdriver.py) (same as [`examples/asterix.ini`](./examples/asterix.ini)), so you can **omit `--config`** and rely on built-in defaults plus optional CLI overrides.

Create a config file passed with `tpcc.py --config` when you prefer an INI. A starting point is [`examples/asterix.ini`](./examples/asterix.ini). Example:

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

- **`dataverse`**: Must match the dataverse used in your DDL (`USE dataverse`). The same name can be passed to **`load_ddl.py --dataverse NAME`** (rewrites `bench` in the bundled `.sqlpp` files) and **`generate_load_sqlpp.py --dataverse NAME`** (or `-D NAME`) for generated loads. For the benchmark run, you can pass **`--dataverse NAME`** (or **`-D NAME`**) to `tpcc.py` to override `[asterix] dataverse` in the INI without editing the file.
- **CLI without INI** (each overrides the corresponding `[asterix]` key when set): **`--asterix-cc-host`**, **`--asterix-cc-port`**, **`--asterix-tls` / `--no-asterix-tls`**, **`--dataverse` / `-D`**, **`--asterix-analytical-subdir`**, **`--asterix-statement-timeout-sec`**, **`--asterix-output-dir`**, **`--asterix-join-hint`**. See `python tpcc.py asterix --help`.
- **`analytical_query_subdir`**: Subfolder under `analytical_queries/` (defaults to `asterix`).
- **`output_dir`**: Used with `--docgen-load` (JSON files per collection). Each JSON line is the document body from `getOneDoc` only (no separate `key` field); primary keys are the fields in `ddl/asterix/ch2pp_bench.sqlpp` `CREATE DATASET ... PRIMARY KEY`.

## Load modes

| Flag | Behavior |
|------|----------|
| `--docgen-load` | Write CH2++ JSON batches to `output_dir` (same layout as `nestcollectionsdocgen`). Then generate `LOAD DATASET` SQL with `scripts/asterix/generate_load_sqlpp.py` and run `scripts/asterix/load_ddl.py`. |
| `--asterix-http-insert` | Insert each generated document with `INSERT INTO dataset ([...])` over HTTP (slow; requires types that match JSON). |

## Example commands

**Run without `--config` (defaults + CLI overrides):**

```bash
python tpcc.py asterix --ch2pp --no-load --asterix-cc-host 192.168.1.10 --dataverse mydv \
  --tclients 0 --aclients 1 --query-iterations 1
```

**1. Generate JSON only (no DB):**

```bash
python tpcc.py nestcollectionsdocgen --ch2pp --warehouses 1 --tclients 1 --no-execute --docgen-load
```

**2. Load JSON into Asterix** (schema first, then bulk load; one `LOAD`/`COPY` per `*.json` shard file—see `ddl/asterix/ch2pp_load_example.sqlpp`):

```bash
python scripts/asterix/load_ddl.py --url http://127.0.0.1:19002/query/service --dataverse mydv --file ddl/asterix/ch2pp_bench.sqlpp
python scripts/asterix/generate_load_sqlpp.py --output-dir /tmp/ch2_data --dataverse mydv --out /tmp/ch2_load.sqlpp
python scripts/asterix/load_ddl.py --url http://127.0.0.1:19002/query/service --dataverse mydv --file /tmp/ch2_load.sqlpp
```

**Arbitrary single dataset (flat `*.json` directory):** use `scripts/asterix/generate_json_dir_load_sqlpp.py` to emit one `LOAD`/`COPY` per file (default: `LOAD DATASET ... USING localfs`; use `--syntax copy` for `COPY ... FROM localfs path ('host://...') with {'format':'json'};` per file). Pass `--nc-host` and paths that exist on the Node Controller for `localfs`. The same `--syntax` flag exists on `generate_load_sqlpp.py` for CH2 multi-dataset loads. Then run `load_ddl.py` on the generated `.sqlpp` as above.

**DDL + bulk load in one command:** `scripts/asterix/asterix_ddl_and_json_load.py` posts a schema file once (`-D mydv` rewrites the dataverse name from `--dataverse-from`, same as `load_ddl.py`), then posts generated load SQL.

- **CH2++ docgen tree** (same layout as `generate_load_sqlpp.py`: `--output-dir` with `warehouse/`, `customer/`, … subdirs of `*.json`): use `--docgen-dir /tmp/ch2_data` and `--nc-host`. One process replaces the old three commands (DDL + `generate_load_sqlpp.py` + second `load_ddl.py`); the script emits **one `LOAD`/`COPY` per JSON shard file**, then `ANALYZE` each loaded dataset, all in one generated script.

- **Single flat `*.json` directory:** use `--json-dir` + `--dataset` (same as `generate_json_dir_load_sqlpp.py`).

Replace `mydv` with your dataverse name, or omit `--dataverse` / `-D` to use the default name `bench` from the DDL files. `load_ddl.py` also accepts **`--dataverse-from OLD`** (default `bench`) if your `.sqlpp` uses another placeholder to rename.

`load_ddl.py` sends **one statement per HTTP request**; a standalone `USE` does not carry over. The script therefore **prepends a `USE` for the active dataverse to each statement** that must run inside it (after parsing `USE` lines in the file), so `CREATE TYPE`, `LOAD DATASET`, and `COPY ... FROM` always execute in the correct dataverse.

Use the same `--output-dir` as `[asterix] output_dir` in your INI. Set `--nc-host` if the Node Controller hostname for `localfs` is not `127.0.0.1`. If Asterix runs in Docker or on remote hosts, that directory must exist on the **NC** machine that reads `localfs`, not only on your laptop.

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

Files live in `analytical_queries/asterix/` (`ch2pp.sql` and optional `ch2pp_non_optimized.sql`). They are derived from the Couchbase N1QL versions with mechanical edits (modulo operator, stripped index hints). Year-from-string fields use `get_year(datetime(...))` instead of N1QL `date_part_str`; offsets use `datetime(...) + duration("PnD")` instead of `date_add_str`. **Validate** on your AsterixDB version before relying on results.

## Environment

- `IGNORE_SKIP_INDEX_HINTS=1` — strip `skip-index` hints if any remain (same idea as Couchbase driver).
- `ACLIENT_REQUEST_PARAMS` — optional JSON merged into POST fields for `/query/service` (advanced).

## Limitations

- Transactional path uses **sequential** statements (no Couchbase-style `BEGIN`/`txid`). Semantics depend on AsterixDB transaction support for your deployment.
- `array_append` and nested `UPDATE` syntax must match your server version; adjust `drivers/asterixdriver.py` if the engine rejects a statement.
