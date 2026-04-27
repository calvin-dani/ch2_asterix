# Asterix CH2++: end-to-end command reference

Run commands from the **`ch2driver/pytpcc`** directory unless noted. Replace:

- `mydv` — your dataverse name  
- `http://127.0.0.1:19002/query/service` — your Cluster Controller query URL  
- `/tmp/ch2_data` — docgen output directory (match everywhere)  
- `10.16.229.105` — **Node Controller** host for `localfs` URIs (JSON must exist on that host at the paths generated)

For **`LOAD DATASET ... USING localfs`** instead of **`COPY`**, use `--syntax load` (or omit; load is the default) on `generate_load_sqlpp.py` and `asterix_ddl_and_json_load.py`. This file uses **`COPY`** because it matches many deployments where `LOAD` fails.

---

## 0. Environment (repo root, once)

```bash
cd /path/to/ch2
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cd ch2driver/pytpcc
```

---

## 1. Generate CH2++ JSON (no database)

```bash
python tpcc.py nestcollectionsdocgen --ch2pp --warehouses 1 --tclients 1 --no-execute --docgen-load
```

Point **`[asterix] output_dir`** in your INI (or the docgen default) at the same tree you use for `--output-dir` / `--docgen-dir` in the next steps (e.g. `/tmp/ch2_data`).

---

## 2. Load data into Asterix

### 2a. Schema (DDL only)

```bash
python scripts/asterix/load_ddl.py \
  --url http://127.0.0.1:19002/query/service \
  --dataverse mydv \
  --file ddl/asterix/ch2pp_bench.sqlpp
```

### 2b. Generate bulk load SQL (COPY)

```bash
python scripts/asterix/generate_load_sqlpp.py \
  --output-dir /tmp/ch2_data \
  --dataverse mydv \
  --nc-host 10.16.229.105 \
  --syntax copy \
  --out /tmp/ch2_load.sqlpp
```

### 2c. Post the load script

```bash
python scripts/asterix/load_ddl.py \
  --url http://127.0.0.1:19002/query/service \
  --dataverse mydv \
  --file /tmp/ch2_load.sqlpp
```

Use `--timeout 0` or a large `--timeout` if jobs run a long time.

### Alternative: DDL + COPY in one process

```bash
python scripts/asterix/asterix_ddl_and_json_load.py \
  --url http://127.0.0.1:19002/query/service \
  --ddl-file ddl/asterix/ch2pp_bench.sqlpp \
  -D mydv \
  --docgen-dir /tmp/ch2_data \
  --nc-host 10.16.229.105 \
  --syntax copy
```

### Single flat JSON directory (optional)

```bash
python scripts/asterix/generate_json_dir_load_sqlpp.py \
  --json-dir /path/to/flat/json \
  --dataset mydataset \
  --dataverse mydv \
  --nc-host 10.16.229.105 \
  --syntax copy \
  --out /tmp/one_dataset_load.sqlpp
```

---

## 3. Full CH2++ benchmark (`tpcc.py` — transactions + analytical queries)

```bash
python tpcc.py asterix --ch2pp --no-load --dataverse mydv \
  --asterix-cc-host 127.0.0.1 \
  --tclients 4 --aclients 1 --query-iterations 2 --warmup-query-iterations 1
```

With an INI:

```bash
python tpcc.py asterix --config examples/asterix.ini --ch2pp --no-load \
  --tclients 4 --aclients 1 --query-iterations 2 --warmup-query-iterations 1
```

(Override **`--dataverse mydv`** if it differs from the INI.)

---

## 4. Ad-hoc HTTP benchmarks (separate from `tpcc.py`)

These time **client HTTP round-trips** to `/query/service`.

### 4a. PK point lookups — generate

```bash
python scripts/asterix/create_point_queries_sqlpp.py -D mydv --min 1 --max 1000 \
  --out /tmp/points_all.sqlpp
```

### 4b. PK point lookups — run

```bash
python scripts/asterix/run_point_queries_benchmark.py \
  --url http://127.0.0.1:19002/query/service \
  -D mydv \
  --file /tmp/points_all.sqlpp \
  --json-summary /tmp/point_benchmark.json
```

### 4c. Orders time-range queries (`o_entry_d`) — generate

```bash
python scripts/asterix/create_orders_time_range_queries_sqlpp.py -D mydv --out-dir /tmp/qr \
  --counts 1:500,5:200,15:100,60:50 --seed 42
```

### 4d. Orders time-range — run all four interval files

```bash
python scripts/asterix/run_orders_time_range_batch.py \
  --url http://127.0.0.1:19002/query/service \
  -D mydv \
  --out-dir /tmp/qr
```

(Or run `run_point_queries_benchmark.py` once per `orders_range_*m.sqlpp` with a different `--json-summary` each time.)

### 4e. Constant query (`SELECT VALUE 1+1;`) N times

```bash
python scripts/asterix/run_constant_query_benchmark.py \
  --url http://127.0.0.1:19002/query/service \
  -D mydv \
  -n 10000 \
  --json-summary /tmp/constant_bench.json
```

---

## See also

- `ASTERIX.md` — configuration and deeper notes  
- `ddl/asterix/README.md` — DDL and load workflow
