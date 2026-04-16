# -*- coding: utf-8 -*-
# Apache AsterixDB driver for CH2++ (SQL++ over HTTP /query/service).

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from pathlib import Path

import requests
import ujson

import constants
from .abstractdriver import AbstractDriver


def _strip_hints(sql: str) -> str:
    return re.sub(r"/\*\+[^*]*\*/", "", sql)


def _lean(s: str) -> str:
    return " ".join(s.split())


class AsterixDriver(AbstractDriver):
    """
    CH2++ driver for Apache AsterixDB.

    - Config via tpcc.config INI section [asterix] (see makeDefaultConfig).
    - Analytical queries: analytical_queries/<analytical_query_subdir>/ch2pp.sql
    - Load: --docgen-load (JSON files, same layout as nestcollectionsdocgen) or
      --asterix-http-insert (INSERT over HTTP; requires matching DDL).
    """

    DEFAULT_CONFIG = {
        "cc_host": ("Cluster Controller hostname", "127.0.0.1"),
        "cc_port": ("Cluster Controller HTTP port", 19002),
        "use_tls": ("Use https:// for /query/service", False),
        "dataverse": ("Default dataverse name (datasets live here)", "bench"),
        "analytical_query_subdir": (
            "Subfolder under analytical_queries/ for ch2pp.sql",
            "asterix",
        ),
        "statement_timeout_sec": (
            "Optional HTTP timeout for each statement (seconds); empty = no timeout",
            "",
        ),
        "output_dir": (
            "For --docgen-load: directory for JSON files (like nestcollectionsdocgen)",
            "/tmp/ch2_data",
        ),
        "join_hint": (
            "Optional join hint token for generated txn SQL (e.g. hash | btree)",
            "",
        ),
    }

    def __init__(
        self,
        ddl,
        clientId,
        TAFlag="T",
        schema=constants.CH2_DRIVER_SCHEMA["CH2"],
        preparedTransactionQueries=None,
        analyticalQueries=constants.CH2_DRIVER_ANALYTICAL_QUERIES[
            "HAND_OPTIMIZED_QUERIES"
        ],
        customerExtraFields=constants.CH2_CUSTOMER_EXTRA_FIELDS["NOT_SET"],
        ordersExtraFields=constants.CH2_ORDERS_EXTRA_FIELDS["NOT_SET"],
        itemExtraFields=constants.CH2_ITEM_EXTRA_FIELDS["NOT_SET"],
        load_mode=constants.CH2_DRIVER_LOAD_MODE["NOT_SET"],
        load_format=constants.CH2_DRIVER_LOAD_FORMAT["JSON"],
        kv_timeout=constants.CH2_DRIVER_KV_TIMEOUT,
        bulkload_batch_size=constants.CH2_DRIVER_BULKLOAD_BATCH_SIZE,
    ):
        super().__init__("asterix", ddl)
        if preparedTransactionQueries is None:
            preparedTransactionQueries = {}
        self.client_id = clientId
        self.TAFlag = TAFlag
        self.schema = schema
        self.analyticalQueries = analyticalQueries
        self.load_mode = load_mode
        self.customerExtraFields = customerExtraFields
        self.ordersExtraFields = ordersExtraFields
        self.itemExtraFields = itemExtraFields
        self.kv_timeout = kv_timeout
        self.bulkload_batch_size = bulkload_batch_size

        self.tx_status = ""
        self.prepared_dict = preparedTransactionQueries
        self.cc_base_url = ""
        self.query_service_url = ""
        self.dataverse = ""
        self.analytical_query_subdir = "asterix"
        self.statement_timeout = None
        self.output_dir: Path | None = None
        self.join_hint = ""
        self.batches: dict = {}
        self._ignore_skip_index_hints = (
            os.environ.get("IGNORE_SKIP_INDEX_HINTS", "0") == "1"
        )

    def makeDefaultConfig(self):
        return AsterixDriver.DEFAULT_CONFIG

    def loadConfig(self, config):
        host = str(config["cc_host"])
        port = int(config["cc_port"])
        tls = config.get("use_tls", False)
        if isinstance(tls, str):
            tls = tls.lower() in ("1", "true", "yes")
        scheme = "https" if tls else "http"
        self.cc_base_url = f"{scheme}://{host}:{port}"
        self.query_service_url = f"{self.cc_base_url}/query/service"
        self.dataverse = str(config.get("dataverse", "bench"))
        self.analytical_query_subdir = str(
            config.get("analytical_query_subdir", "asterix")
        )
        to = config.get("statement_timeout_sec", "")
        self.statement_timeout = float(to) if str(to).strip() != "" else None
        self.join_hint = str(config.get("join_hint", "") or "").strip()
        if self.join_hint and not self.join_hint.startswith(" "):
            self.join_hint = " " + self.join_hint + " "
        else:
            self.join_hint = self.join_hint or " "
        out = config.get("output_dir", "/tmp/ch2_data")
        self.output_dir = Path(out)

    def txStatus(self):
        return self.tx_status

    def execute_sqlpp(self, statement: str, timeout=None):
        """POST a SQL++ statement; return parsed JSON response."""
        stmt = _lean(statement)
        if not stmt:
            return {"status": "empty", "errors": ["empty statement"]}
        data = {"statement": stmt}
        t = timeout if timeout is not None else self.statement_timeout
        try:
            r = requests.post(self.query_service_url, data=data, timeout=t)
            r.raise_for_status()
            body = r.json()
            return body
        except requests.RequestException as ex:
            logging.warning("Asterix HTTP error: %s", ex)
            return {"status": "fatal", "errors": [str(ex)]}

    def _use_prefix(self) -> str:
        return f"USE {self.dataverse};"

    def _ds(self, table_name: str) -> str:
        """Fully qualified dataset name: dataverse.dataset"""
        coll = constants.COLLECTIONS_DICT.get(table_name, table_name)
        return f"{self.dataverse}.{coll}"

    # --- load: docgen (JSON files) or HTTP insert ---

    def _get_batch_file(self, tableName: str, batch_idx: int) -> Path:
        assert self.output_dir is not None
        return self.output_dir / tableName / (
            "%s-%d-%d.json" % (tableName, self.client_id, batch_idx)
        )

    def _save_tuples_file(self, filename, tuples_lines, mode="w"):
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        with open(filename, mode) as f:
            f.writelines(tuples_lines)
        return True

    def loadTuples(self, tableName, tuples):
        if len(tuples) == 0:
            return
        assert tableName in constants.ALL_TABLES, "Unexpected table %s" % tableName

        if self.load_mode == constants.CH2_DRIVER_LOAD_MODE["DOCGEN_LOAD"]:
            batch_idx, cur_batch, batch_size = self.batches.get(
                tableName, [0, [], 0]
            )
            for t in tuples:
                _, val = self.getOneDoc(tableName, t, generateKey=True)
                json_val = ujson.dumps(val) + "\n"
                cur_batch.append(json_val)
                batch_size += len(json_val)
                if batch_size > self.bulkload_batch_size:
                    ok = self._save_tuples_file(
                        self._get_batch_file(tableName, batch_idx), cur_batch
                    )
                    if ok:
                        batch_idx += 1
                        cur_batch = []
                        batch_size = 0
                    else:
                        logging.error("Failed writing batch for %s", tableName)
                        return
            self.batches[tableName] = [batch_idx, cur_batch, batch_size]
            return

        if self.load_mode == constants.CH2_DRIVER_LOAD_MODE["ASTERIX_HTTP_INSERT"]:
            coll = constants.COLLECTIONS_DICT.get(tableName, tableName)
            for t in tuples:
                _key, val = self.getOneDoc(tableName, t, generateKey=False)
                payload = ujson.dumps(val)
                stmt = f"{self._use_prefix()} INSERT INTO {coll} ([ {payload} ]);"
                body = self.execute_sqlpp(stmt)
                if body.get("status") != "success":
                    logging.error(
                        "INSERT failed for %s: %s", tableName, body.get("errors", body)
                    )
            return

        logging.error(
            "AsterixDriver: unsupported load_mode for loadTuples: %s", self.load_mode
        )
        sys.exit(1)

    def _load_finish_leader(self):
        assert self.output_dir is not None
        for tableName, (batch_idx, cur_batch, _) in self.batches.items():
            lockfile = self.output_dir / tableName / (".c%d.lock" % self.client_id)
            lockfile.parent.mkdir(parents=True, exist_ok=True)
            with open(lockfile, "w") as f:
                f.write(f"{self.client_id} {batch_idx}")
            if cur_batch:
                batch_file = self._get_batch_file(tableName, batch_idx)
                batch_file.touch()
                self._save_tuples_file(batch_file, cur_batch, mode="a")
            lockfile.rename(lockfile.parent / (".c%d.lock" % (self.client_id + 1)))

    def _load_finish_follower(self):
        assert self.output_dir is not None
        for tableName, (_, cur_batch, batch_size) in self.batches.items():
            lockfile = self.output_dir / tableName / (".c%d.lock" % self.client_id)
            while not lockfile.exists():
                time.sleep(0.1)
            client_id, batch_idx = map(int, lockfile.read_text().split())
            if cur_batch:
                batch_file = self._get_batch_file(tableName, batch_idx)
                file_size = batch_file.stat().st_size
                remaining = self.bulkload_batch_size - file_size
                if batch_size < remaining:
                    self._save_tuples_file(batch_file, cur_batch, mode="a")
                else:
                    tail_idx, head_size = 0, 0
                    while head_size < remaining:
                        head_size += len(cur_batch[tail_idx])
                        tail_idx += 1
                    self._save_tuples_file(batch_file, cur_batch[:tail_idx], mode="a")
                    batch_idx += 1
                    new_batch_file = self._get_batch_file(tableName, batch_idx)
                    self._save_tuples_file(new_batch_file, cur_batch[tail_idx:])
                    lockfile.write_text(f"{client_id} {batch_idx}")
            lockfile.rename(lockfile.parent / (".c%d.lock" % (self.client_id + 1)))

    def loadFinish(self):
        if self.load_mode != constants.CH2_DRIVER_LOAD_MODE["DOCGEN_LOAD"]:
            return
        logging.info("Client ID # %d Writing last batches to disk" % self.client_id)
        if self.client_id == 0:
            self._load_finish_leader()
        else:
            self._load_finish_follower()
        logging.info("Client ID # %d Finished loading tables" % self.client_id)

    def loadStart(self):
        if self.load_mode == constants.CH2_DRIVER_LOAD_MODE["DOCGEN_LOAD"]:
            assert self.output_dir is not None
            self.output_dir.mkdir(parents=True, exist_ok=True)

    # --- analytical ---

    def runCH2Queries(self, duration, endBenchmarkTime, queryIterNum):
        qry_times = {}
        if self.TAFlag != "A":
            return qry_times

        try:
            request_params = json.loads(os.environ.get("ACLIENT_REQUEST_PARAMS", "{}"))
        except json.JSONDecodeError:
            logging.warning("Failed to decode ACLIENT_REQUEST_PARAMS")
            request_params = {}

        ch2_queries_perm = constants.CH2_QUERIES_PERM[self.client_id]
        subdir = self.analytical_query_subdir
        ch2_queries = self.loadAnalyticalQueriesFromFile(subdir)

        if self._ignore_skip_index_hints:
            pattern = re.compile(r"/\*\+\s*skip-index\s*\*/")
            ch2_queries = {k: re.sub(pattern, "", v) for k, v in ch2_queries.items()}

        for qry in ch2_queries_perm:
            query_id_str = "AClient %d:Loop %d:%s:" % (
                self.client_id + 1,
                queryIterNum + 1,
                qry,
            )
            query_body = ch2_queries[qry]
            query_body = _strip_hints(query_body)
            full_stmt = f"{self._use_prefix()}\n{query_body}"

            start = time.time()
            startTime = time.strftime("%H:%M:%S", time.localtime(start))
            if duration is not None and start > endBenchmarkTime:
                logging.debug(
                    "%s started after benchmark window", query_id_str
                )
                break

            logging.info("%s started at: %s", query_id_str, startTime)
            data = {"statement": _lean(full_stmt)}
            data.update(request_params)
            try:
                r = requests.post(
                    self.query_service_url,
                    data=data,
                    timeout=self.statement_timeout,
                )
                body = r.json() if r.content else {}
            except Exception as ex:
                logging.error("%s request failed: %s", query_id_str, ex)
                body = {"status": "fatal", "errors": [str(ex)]}

            end = time.time()
            endTime = time.strftime("%H:%M:%S", time.localtime(end))
            if duration is not None and end > endBenchmarkTime:
                logging.debug("%s ended after benchmark window", query_id_str)
                break

            logging.info("%s ended at: %s", query_id_str, endTime)
            logging.info(
                "%s metrics: %s",
                query_id_str,
                body.get("metrics"),
            )
            if body.get("status") != "success":
                logging.warning(
                    "%s errors: %s",
                    query_id_str,
                    json.dumps(body.get("errors", [])),
                )

            qry_times[qry] = [
                self.client_id + 1,
                queryIterNum + 1,
                startTime,
                body.get("metrics", {}).get("executionTime", "n/a"),
                endTime,
            ]
        return qry_times

    # --- TPC-C transactions (CH2++ SQL++; sequential statements, no Couchbase txids) ---

    def _q(self, sql: str):
        body = self.execute_sqlpp(sql)
        ok = body.get("status") == "success"
        if not ok:
            self.tx_status = "error"
        return body, ok

    def _esc(self, s) -> str:
        if s is None:
            return ""
        return str(s).replace("'", "''")

    def _dt(self, d) -> str:
        if hasattr(d, "strftime"):
            return d.strftime("%Y-%m-%d %H:%M:%S.%f")
        return str(d)

    def doDelivery(self, params):
        self.tx_status = ""
        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]
        ol_delivery_d = self._dt(params["ol_delivery_d"])
        ds = self._ds
        result = []
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1):
            q1 = f"""{self._use_prefix()}
            SELECT no_o_id FROM {ds(constants.TABLENAME_NEWORDER)}
            WHERE no_d_id = {d_id} AND no_w_id = {w_id} AND no_o_id > -1 LIMIT 1;"""
            body, ok = self._q(q1)
            if not ok or not body.get("results"):
                continue
            no_o_id = body["results"][0].get("no_o_id")
            if no_o_id is None:
                continue

            q2 = f"""{self._use_prefix()}
            SELECT o_c_id FROM {ds(constants.TABLENAME_ORDERS)}
            WHERE o_id = {no_o_id} AND o_d_id = {d_id} AND o_w_id = {w_id};"""
            body, ok = self._q(q2)
            if not ok or not body.get("results"):
                continue
            c_id = body["results"][0]["o_c_id"]

            qsum = f"""{self._use_prefix()}
            SELECT VALUE SUM(ol.ol_amount)
            FROM {ds(constants.TABLENAME_ORDERS)} o, o.o_orderline ol
            WHERE o.o_id = {no_o_id} AND o.o_d_id = {d_id} AND o.o_w_id = {w_id};"""
            body, ok = self._q(qsum)
            if not ok:
                continue
            r0 = body.get("results", [None])[0]
            ol_total = float(r0) if r0 is not None else 0.0

            qd = f"""{self._use_prefix()}
            DELETE FROM {ds(constants.TABLENAME_NEWORDER)}
            WHERE no_d_id = {d_id} AND no_w_id = {w_id} AND no_o_id = {no_o_id};"""
            body, ok = self._q(qd)
            if not ok:
                continue

            qu = f"""{self._use_prefix()}
            UPDATE {ds(constants.TABLENAME_ORDERS)} SET o_carrier_id = {o_carrier_id}
            WHERE o_id = {no_o_id} AND o_d_id = {d_id} AND o_w_id = {w_id};"""
            body, ok = self._q(qu)
            if not ok:
                continue

            qu2 = f"""{self._use_prefix()}
            UPDATE {ds(constants.TABLENAME_ORDERS)} o
            SET ol.ol_delivery_d = '{ol_delivery_d}'
            FOR ol IN o.o_orderline
            WHERE o.o_id = {no_o_id} AND o.o_d_id = {d_id} AND o.o_w_id = {w_id};"""
            body, ok = self._q(qu2)
            if not ok:
                continue

            quc = f"""{self._use_prefix()}
            UPDATE {ds(constants.TABLENAME_CUSTOMER)} SET c_balance = c_balance + {ol_total}
            WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id};"""
            body, ok = self._q(quc)
            if not ok:
                continue

            result.append((d_id, no_o_id))
        return result

    def doNewOrder(self, params):
        self.tx_status = ""
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        o_entry_d = self._dt(params["o_entry_d"])
        i_ids = params["i_ids"]
        i_w_ids = params["i_w_ids"]
        i_qtys = params["i_qtys"]
        ds = self._ds

        for i_id in i_ids:
            qb = f"""{self._use_prefix()}
            SELECT i_price, i_name, i_data FROM {ds(constants.TABLENAME_ITEM)} WHERE i_id = {i_id};"""
            body, ok = self._q(qb)
            if not ok or not body.get("results"):
                self.tx_status = "assert"
                return

        qw = f"""{self._use_prefix()} SELECT w_tax FROM {ds(constants.TABLENAME_WAREHOUSE)} WHERE w_id = {w_id};"""
        body, ok = self._q(qw)
        if not ok or not body.get("results"):
            return
        w_tax = body["results"][0]["w_tax"]

        qd = f"""{self._use_prefix()}
        SELECT d_tax, d_next_o_id FROM {ds(constants.TABLENAME_DISTRICT)}
        WHERE d_id = {d_id} AND d_w_id = {w_id};"""
        body, ok = self._q(qd)
        if not ok or not body.get("results"):
            return
        d_tax = body["results"][0]["d_tax"]
        d_next_o_id = body["results"][0]["d_next_o_id"]

        qc = f"""{self._use_prefix()}
        SELECT c_discount, c_name.c_last, c_credit FROM {ds(constants.TABLENAME_CUSTOMER)}
        WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id};"""
        body, ok = self._q(qc)
        if not ok or not body.get("results"):
            return
        c_discount = body["results"][0]["c_discount"]

        self._q(
            f"""{self._use_prefix()}
            UPDATE {ds(constants.TABLENAME_DISTRICT)} SET d_next_o_id = {d_next_o_id + 1}
            WHERE d_id = {d_id} AND d_w_id = {w_id};"""
        )

        ol_cnt = len(i_ids)
        o_carrier_id = constants.NULL_CARRIER_ID
        all_local = all(i_w_ids[i] == w_id for i in range(len(i_ids)))
        ins_o = f"""{self._use_prefix()}
        INSERT INTO {ds(constants.TABLENAME_ORDERS)} ([ {{
          "o_id": {d_next_o_id}, "o_d_id": {d_id}, "o_w_id": {w_id},
          "o_c_id": {c_id}, "o_entry_d": "{o_entry_d}", "o_carrier_id": {o_carrier_id},
          "o_ol_cnt": {ol_cnt}, "o_all_local": {str(all_local).lower()},
          "o_orderline": []
        }} ]);"""
        body, ok = self._q(ins_o)
        if not ok:
            return

        self._q(
            f"""{self._use_prefix()}
            INSERT INTO {ds(constants.TABLENAME_NEWORDER)} ([ {{
              "no_o_id": {d_next_o_id}, "no_d_id": {d_id}, "no_w_id": {w_id}
            }} ]);"""
        )

        item_data = []
        total = 0.0
        for i in range(len(i_ids)):
            ol_number = i + 1
            ol_supply_w_id = i_w_ids[i]
            ol_i_id = i_ids[i]
            ol_quantity = i_qtys[i]

            qi = f"""{self._use_prefix()}
            SELECT i_price, i_name, i_data FROM {ds(constants.TABLENAME_ITEM)} WHERE i_id = {ol_i_id};"""
            body, ok = self._q(qi)
            itemInfo = body["results"][0]
            i_name = itemInfo["i_name"]
            i_data = itemInfo["i_data"]
            i_price = itemInfo["i_price"]

            qs = f"""{self._use_prefix()}
            SELECT s_quantity, s_data, s_ytd, s_order_cnt, s_remote_cnt, s_dists
            FROM {ds(constants.TABLENAME_STOCK)}
            WHERE s_i_id = {ol_i_id} AND s_w_id = {ol_supply_w_id};"""
            body, ok = self._q(qs)
            if not ok or not body.get("results"):
                return
            st = body["results"][0]
            s_quantity = st["s_quantity"]
            s_ytd = st["s_ytd"]
            s_order_cnt = st["s_order_cnt"]
            s_remote_cnt = st["s_remote_cnt"]
            s_data = st["s_data"]
            s_dists = st["s_dists"]
            s_dist_xx = s_dists[d_id - 1]

            s_ytd += ol_quantity
            if s_quantity >= ol_quantity + 10:
                s_quantity = s_quantity - ol_quantity
            else:
                s_quantity = s_quantity + 91 - ol_quantity
            s_order_cnt += 1
            if ol_supply_w_id != w_id:
                s_remote_cnt += 1

            self._q(
                f"""{self._use_prefix()}
                UPDATE {ds(constants.TABLENAME_STOCK)}
                SET s_quantity = {s_quantity}, s_ytd = {s_ytd},
                    s_order_cnt = {s_order_cnt}, s_remote_cnt = {s_remote_cnt}
                WHERE s_i_id = {ol_i_id} AND s_w_id = {ol_supply_w_id};"""
            )

            if constants.ORIGINAL_STRING in i_data and constants.ORIGINAL_STRING in s_data:
                brand_generic = "B"
            else:
                brand_generic = "G"
            ol_amount = ol_quantity * i_price
            total += ol_amount

            up = f"""{self._use_prefix()}
            UPDATE {ds(constants.TABLENAME_ORDERS)} o
            SET o_orderline = array_append(o.o_orderline, {{
              "ol_number": {ol_number}, "ol_i_id": {ol_i_id},
              "ol_supply_w_id": {ol_supply_w_id}, "ol_delivery_d": "{o_entry_d}",
              "ol_quantity": {ol_quantity}, "ol_amount": {ol_amount},
              "ol_dist_info": "{self._esc(s_dist_xx)}"
            }})
            WHERE o.o_id = {d_next_o_id} AND o.o_d_id = {d_id} AND o.o_w_id = {w_id};"""
            self._q(up)
            item_data.append((i_name, s_quantity, brand_generic, i_price, ol_amount))

        total *= (1 - c_discount) * (1 + w_tax + d_tax)
        misc = [(w_tax, d_tax, d_next_o_id, total)]
        return [[{"w_tax": w_tax}], misc, item_data]

    def doOrderStatus(self, params):
        self.tx_status = ""
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        ds = self._ds

        if c_id is not None:
            q = f"""{self._use_prefix()}
            SELECT c_id, c_name.c_first, c_name.c_middle, c_name.c_last, c_balance
            FROM {ds(constants.TABLENAME_CUSTOMER)}
            WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id};"""
            body, ok = self._q(q)
            if not ok or not body.get("results"):
                self.tx_status = "assert"
                return
            customer = body["results"][0]
        else:
            q = f"""{self._use_prefix()}
            SELECT c_id, c_name.c_first, c_name.c_middle, c_name.c_last, c_balance
            FROM {ds(constants.TABLENAME_CUSTOMER)}
            WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_name.c_last = '{self._esc(c_last)}'
            ORDER BY c_name.c_first;"""
            body, ok = self._q(q)
            if not ok or not body.get("results"):
                self.tx_status = "assert"
                return
            rows = body["results"]
            customer = rows[(len(rows) - 1) // 2]
            c_id = customer["c_id"]

        qo = f"""{self._use_prefix()}
        SELECT o_id, o_carrier_id, o_entry_d FROM {ds(constants.TABLENAME_ORDERS)}
        WHERE o_w_id = {w_id} AND o_d_id = {d_id} AND o_c_id = {c_id}
        ORDER BY o_id DESC LIMIT 1;"""
        body, ok = self._q(qo)
        order = body.get("results", [])
        if order:
            oid = order[0]["o_id"]
            ql = f"""{self._use_prefix()}
            SELECT ol.ol_supply_w_id, ol.ol_i_id, ol.ol_quantity, ol.ol_amount, ol.ol_delivery_d
            FROM {ds(constants.TABLENAME_ORDERS)} o, o.o_orderline ol
            WHERE o.o_w_id = {w_id} AND o.o_d_id = {d_id} AND o.o_id = {oid};"""
            body, ok = self._q(ql)
            orderLines = body.get("results", [])
        else:
            orderLines = []
        return [customer, order, orderLines]

    def doPayment(self, params):
        self.tx_status = ""
        w_id = params["w_id"]
        d_id = params["d_id"]
        h_amount = params["h_amount"]
        c_w_id = params["c_w_id"]
        c_d_id = params["c_d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        h_date = self._dt(params["h_date"])
        ds = self._ds

        if c_id is not None:
            q = f"""{self._use_prefix()}
            SELECT c.c_id, c.c_name.c_first, c.c_name.c_middle, c.c_name.c_last,
                   ca.c_street_1, ca.c_street_2, ca.c_city, ca.c_state, ca.c_zip,
                   cp.c_phone_number, c.c_since, c.c_credit, c.c_credit_lim,
                   c.c_discount, c.c_balance, c.c_ytd_payment, c.c_payment_cnt, c.c_data
            FROM {ds(constants.TABLENAME_CUSTOMER)} c, c.c_addresses ca, c.c_phones cp
            WHERE c.c_w_id = {w_id} AND c.c_d_id = {d_id} AND c.c_id = {c_id}
              AND ca.c_address_kind = 'shipping' AND cp.c_phone_kind = 'contact';"""
            body, ok = self._q(q)
            if not ok or not body.get("results"):
                self.tx_status = "assert"
                return
            customer = body["results"][0]
        else:
            q = f"""{self._use_prefix()}
            SELECT c.c_id, c.c_name.c_first, c.c_name.c_middle, c.c_name.c_last,
                   ca.c_street_1, ca.c_street_2, ca.c_city, ca.c_state, ca.c_zip,
                   cp.c_phone_number, c.c_since, c.c_credit, c.c_credit_lim,
                   c.c_discount, c.c_balance, c.c_ytd_payment, c.c_payment_cnt, c.c_data
            FROM {ds(constants.TABLENAME_CUSTOMER)} c, c.c_addresses ca, c.c_phones cp
            WHERE c.c_w_id = {w_id} AND c.c_d_id = {d_id} AND c.c_name.c_last = '{self._esc(c_last)}'
              AND ca.c_address_kind = 'shipping' AND cp.c_phone_kind = 'contact'
            ORDER BY c.c_name.c_first;"""
            body, ok = self._q(q)
            if not ok or not body.get("results"):
                self.tx_status = "assert"
                return
            rows = body["results"]
            customer = rows[(len(rows) - 1) // 2]
            c_id = customer["c_id"]

        c_balance = customer["c_balance"] - h_amount
        c_ytd_payment = customer["c_ytd_payment"] + h_amount
        c_payment_cnt = customer["c_payment_cnt"] + 1
        c_data = customer["c_data"]

        wh = f"""{self._use_prefix()}
        SELECT w_name, w_address.w_street_1, w_address.w_street_2, w_address.w_city, w_address.w_state, w_address.w_zip
        FROM {ds(constants.TABLENAME_WAREHOUSE)} WHERE w_id = {w_id};"""
        body, ok = self._q(wh)
        if not ok:
            return
        warehouse = body["results"]

        di = f"""{self._use_prefix()}
        SELECT d_name, d_address.d_street_1, d_address.d_street_2, d_address.d_city, d_address.d_state, d_address.d_zip
        FROM {ds(constants.TABLENAME_DISTRICT)} WHERE d_w_id = {w_id} AND d_id = {d_id};"""
        body, ok = self._q(di)
        if not ok:
            return
        district = body["results"]

        self._q(
            f"""{self._use_prefix()}
            UPDATE {ds(constants.TABLENAME_WAREHOUSE)} SET w_ytd = w_ytd + {h_amount} WHERE w_id = {w_id};"""
        )
        self._q(
            f"""{self._use_prefix()}
            UPDATE {ds(constants.TABLENAME_DISTRICT)} SET d_ytd = d_ytd + {h_amount}
            WHERE d_w_id = {w_id} AND d_id = {d_id};"""
        )

        if customer["c_credit"] == constants.BAD_CREDIT:
            newData = " ".join(
                map(str, [c_id, c_d_id, c_w_id, d_id, w_id, h_amount])
            )
            c_data = (newData + "|" + str(c_data))[: constants.MAX_C_DATA]
            self._q(
                f"""{self._use_prefix()}
                UPDATE {ds(constants.TABLENAME_CUSTOMER)}
                SET c_balance = {c_balance}, c_ytd_payment = {c_ytd_payment},
                    c_payment_cnt = {c_payment_cnt}, c_data = '{self._esc(c_data)}'
                WHERE c_w_id = {c_w_id} AND c_d_id = {c_d_id} AND c_id = {c_id};"""
            )
        else:
            self._q(
                f"""{self._use_prefix()}
                UPDATE {ds(constants.TABLENAME_CUSTOMER)}
                SET c_balance = {c_balance}, c_ytd_payment = {c_ytd_payment},
                    c_payment_cnt = {c_payment_cnt}
                WHERE c_w_id = {c_w_id} AND c_d_id = {c_d_id} AND c_id = {c_id};"""
            )

        h_data = f"{warehouse[0]['w_name']}    {district[0]['d_name']}"
        self._q(
            f"""{self._use_prefix()}
            INSERT INTO {ds(constants.TABLENAME_HISTORY)} ([ {{
              "h_c_id": {c_id}, "h_c_d_id": {c_d_id}, "h_c_w_id": {c_w_id},
              "h_d_id": {d_id}, "h_w_id": {w_id}, "h_date": "{h_date}",
              "h_amount": {h_amount}, "h_data": "{self._esc(h_data)}"
            }} ]);"""
        )
        return [warehouse, district, customer]

    def doStockLevel(self, params):
        self.tx_status = ""
        w_id = params["w_id"]
        d_id = params["d_id"]
        threshold = params["threshold"]
        ds = self._ds

        body, ok = self._q(
            f"""{self._use_prefix()}
            SELECT d_next_o_id FROM {ds(constants.TABLENAME_DISTRICT)}
            WHERE d_w_id = {w_id} AND d_id = {d_id};"""
        )
        if not ok or not body.get("results"):
            return 0
        o_id = body["results"][0]["d_next_o_id"]

        q = f"""{self._use_prefix()}
        SELECT VALUE COUNT(DISTINCT ol.ol_i_id)
        FROM {ds(constants.TABLENAME_ORDERS)} o, o.o_orderline ol,
             {ds(constants.TABLENAME_STOCK)} s
        WHERE o.o_w_id = {w_id} AND o.o_d_id = {d_id}
          AND o.o_id < {o_id} AND o.o_id >= {o_id - 20}
          AND s.s_w_id = {w_id} AND s.s_i_id = ol.ol_i_id
          AND s.s_quantity < {threshold};"""
        body, ok = self._q(q)
        if not ok or not body.get("results"):
            return 0
        r0 = body["results"][0]
        return int(r0) if r0 is not None else 0
