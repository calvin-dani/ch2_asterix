--Q01
SELECT
  ol.ol_number,
  sum(ol.ol_quantity) AS sum_qty,
  sum(ol.ol_amount) AS sum_amount,
  avg(ol.ol_quantity) AS avg_qty,
  avg(ol.ol_amount) AS avg_amount,
  count(*) AS count_order
FROM orders o, o.o_orderline ol
WHERE ol.ol_delivery_d > '2014-07-01 00:00:00'
GROUP BY ol.ol_number
ORDER BY ol.ol_number;

--Q02
SELECT
  su.su_suppkey,
  su.su_name,
  n.n_name,
  i.i_id,
  i.i_name,
  su.su_address,
  su.su_phone,
  su.su_comment
FROM
  item i,
  supplier su,
  stock s,
  nation n,
  region r,
  (
    SELECT s1.s_i_id AS m_i_id, min(s1.s_quantity) AS m_s_quantity
    FROM stock s1, supplier su1, nation n1, region r1
    WHERE
      s1.s_w_id * s1.s_i_id % 10000 = su1.su_suppkey
      AND su1.su_nationkey = n1.n_nationkey
      AND n1.n_regionkey = r1.r_regionkey
      AND r1.r_name LIKE 'Europ%'
    GROUP BY s1.s_i_id
  ) m
WHERE
  i.i_id = s.s_i_id
  AND s.s_w_id * s.s_i_id % 10000 = su.su_suppkey
  AND su.su_nationkey = n.n_nationkey
  AND n.n_regionkey = r.r_regionkey
  AND i.i_data LIKE '%b'
  AND r.r_name LIKE 'Europ%'
  AND i.i_id = m.m_i_id
  AND s.s_quantity = m.m_s_quantity
ORDER BY n.n_name, su.su_name, i.i_id
LIMIT 100;

--Q03
SELECT o.o_id, o.o_w_id, o.o_d_id, sum(ol.ol_amount) AS revenue, o.o_entry_d
FROM customer c, c.c_addresses ca, neworder no, orders o, o.o_orderline ol
WHERE
  ca.c_address_kind = 'shipping'
  AND ca.c_state LIKE 'a%'
  AND c.c_id = o.o_c_id
  AND c.c_w_id = o.o_w_id
  AND c.c_d_id = o.o_d_id
  AND no.no_w_id = o.o_w_id
  AND no.no_d_id = o.o_d_id
  AND no.no_o_id = o.o_id
  AND o.o_entry_d < '2017-03-15 00:00:00.000000'
GROUP BY o.o_id, o.o_w_id, o.o_d_id, o.o_entry_d
ORDER BY revenue DESC, o.o_entry_d;

--Q04
SELECT o.o_ol_cnt, count(*) AS order_count
FROM orders o
WHERE
  o.o_entry_d >= '2015-07-01 00:00:00.000000'
  AND o.o_entry_d < '2015-10-01 00:00:00.000000'
  AND EXISTS (
    SELECT VALUE 1
    FROM o.o_orderline ol
    WHERE datetime(ol.ol_delivery_d) >= datetime(o.o_entry_d) + duration("P7D")
  )
GROUP BY o.o_ol_cnt
ORDER BY o.o_ol_cnt;

--Q05
SELECT n.n_name, round(sum(ol.ol_amount), 2) AS revenue
FROM
  customer c,
  c.c_addresses ca,
  orders o,
  o.o_orderline ol,
  stock s,
  supplier su,
  nation n,
  region r
WHERE
  c.c_id = o.o_c_id
  AND c.c_w_id = o.o_w_id
  AND c.c_d_id = o.o_d_id
  AND o.o_w_id = s.s_w_id
  AND ol.ol_i_id = s.s_i_id
  AND s.s_w_id * s.s_i_id % 10000 = su.su_suppkey
  AND ca.c_address_kind = 'shipping'
  AND string_to_codepoint(ca.c_state)[0] = su.su_nationkey
  AND string_to_codepoint(ca.c_state)[0] = n.n_nationkey
  AND su.su_nationkey = n.n_nationkey
  AND n.n_regionkey = r.r_regionkey
  AND r.r_name = 'Asia'
  AND o.o_entry_d >= '2016-01-01 00:00:00.000000'
  AND o.o_entry_d < '2017-01-01 00:00:00.000000'
GROUP BY n.n_name
ORDER BY revenue DESC;

--Q06
SELECT sum(ol.ol_amount) AS revenue
FROM orders o, o.o_orderline ol
WHERE
  ol.ol_delivery_d >= '2016-01-01 00:00:00.000000'
  AND ol.ol_delivery_d < '2017-01-01 00:00:00.000000'
  AND ol.ol_amount > 600;

--Q07
SELECT
  su.su_nationkey AS supp_nation,
  substr1(ca.c_state, 1, 1) AS cust_nation,
  get_year(datetime(o.o_entry_d)) AS l_year,
  round(sum(ol.ol_amount), 2) AS revenue
FROM
  supplier su,
  stock s,
  orders o,
  o.o_orderline ol,
  customer c,
  c.c_addresses ca,
  nation n1,
  nation n2
WHERE
  ol.ol_supply_w_id = s.s_w_id
  AND ol.ol_i_id = s.s_i_id
  AND s.s_w_id * s.s_i_id % 10000 = su.su_suppkey
  AND c.c_id = o.o_c_id
  AND c.c_w_id = o.o_w_id
  AND c.c_d_id = o.o_d_id
  AND su.su_nationkey = n1.n_nationkey
  AND ca.c_address_kind = 'shipping'
  AND string_to_codepoint(ca.c_state)[0] = n2.n_nationkey
  AND (
    (
      n1.n_name = 'Germany'
      AND n2.n_name = 'Cambodia'
    )
    OR (
      n1.n_name = 'Cambodia'
      AND n2.n_name = 'Germany'
    )
  )
  AND ol.ol_delivery_d BETWEEN '2017-01-01 00:00:00.000000' AND '2018-12-31 00:00:00.000000'
GROUP BY su.su_nationkey, substr1(ca.c_state, 1, 1), get_year(datetime(o.o_entry_d))
ORDER BY su.su_nationkey, cust_nation, l_year;

--Q08
SELECT
  get_year(datetime(o.o_entry_d)) AS l_year,
  round(
    (
      sum(
        CASE WHEN n2.n_name = 'Germany' THEN ol.ol_amount ELSE 0 END
      ) / sum(ol.ol_amount)
    ),
    2
  ) AS mkt_share
FROM
  item i,
  supplier su,
  stock s,
  orders o,
  o.o_orderline ol,
  customer c,
  c.c_addresses ca,
  nation n1,
  nation n2,
  region r
WHERE
  i.i_id = s.s_i_id
  AND ol.ol_i_id = s.s_i_id
  AND ol.ol_supply_w_id = s.s_w_id
  AND s.s_w_id * s.s_i_id % 10000 = su.su_suppkey
  AND c.c_id = o.o_c_id
  AND c.c_w_id = o.o_w_id
  AND c.c_d_id = o.o_d_id
  AND ca.c_address_kind = 'shipping'
  AND n1.n_nationkey = string_to_codepoint(ca.c_state)[0]
  AND n1.n_regionkey = r.r_regionkey
  AND ol.ol_i_id < 1000
  AND r.r_name = 'Europe'
  AND su.su_nationkey = n2.n_nationkey
  AND o.o_entry_d BETWEEN '2017-01-01 00:00:00.000000'
  AND '2018-12-31 00:00:00.000000'
  AND i.i_data LIKE '%b'
  AND i.i_id = ol.ol_i_id
GROUP BY get_year(datetime(o.o_entry_d))
ORDER BY l_year;

--Q09
SELECT
  n.n_name,
  get_year(datetime(o.o_entry_d)) AS l_year,
  sum(ol.ol_amount) AS sum_profit
FROM item i, stock s, supplier su, orders o, o.o_orderline ol, nation n
WHERE
  ol.ol_i_id = s.s_i_id
  AND ol.ol_supply_w_id = s.s_w_id
  AND s.s_w_id * s.s_i_id % 10000 = su.su_suppkey
  AND ol.ol_i_id = i.i_id
  AND su.su_nationkey = n.n_nationkey
  AND i.i_data LIKE '%bb'
GROUP BY n.n_name, get_year(datetime(o.o_entry_d))
ORDER BY n.n_name, l_year DESC;

--Q10
SELECT
  c.c_id,
  c.c_name.c_last,
  sum(ol.ol_amount) AS revenue,
  ca.c_city,
  cp.c_phone_number,
  n.n_name
FROM
  customer c,
  c.c_addresses ca,
  c.c_phones cp,
  orders o,
  o.o_orderline ol,
  nation n
WHERE
  c.c_id = o.o_c_id
  AND c.c_w_id = o.o_w_id
  AND c.c_d_id = o.o_d_id
  AND o.o_entry_d >= '2015-10-01 00:00:00.000000'
  AND o.o_entry_d < '2016-01-01 00:00:00.000000'
  AND ca.c_address_kind = 'shipping'
  AND cp.c_phone_kind = 'contact'
  AND n.n_nationkey = string_to_codepoint(ca.c_state)[0]
GROUP BY c.c_id, c.c_name.c_last, ca.c_city, cp.c_phone_number, n.n_name
ORDER BY revenue DESC
LIMIT 20;

--Q11
SELECT s.s_i_id, sum(s.s_order_cnt) AS ordercount
FROM stock s, supplier su, nation n
WHERE
  s.s_w_id * s.s_i_id % 10000 = su.su_suppkey
  AND su.su_nationkey = n.n_nationkey
  AND n.n_name = 'Germany'
GROUP BY s.s_i_id
HAVING
  sum(s.s_order_cnt) > (
    SELECT VALUE sum(s1.s_order_cnt) * 0.00005
    FROM stock s1, supplier su1, nation n1
    WHERE
      s1.s_w_id * s1.s_i_id % 10000 = su1.su_suppkey
      AND su1.su_nationkey = n1.n_nationkey
      AND n1.n_name = 'Germany'
  )[0]
ORDER BY ordercount DESC;

--Q12
SELECT
  o.o_ol_cnt,
  sum(
    CASE WHEN o.o_carrier_id = 1
    OR o.o_carrier_id = 2 THEN 1 ELSE 0 END
  ) AS high_line_count,
  sum(
    CASE WHEN o.o_carrier_id <> 1
    AND o.o_carrier_id <> 2 THEN 1 ELSE 0 END
  ) AS low_line_count
FROM orders o, o.o_orderline ol
WHERE
  o.o_entry_d <= ol.ol_delivery_d
  AND ol.ol_delivery_d >= '2016-01-01 00:00:00.000000'
  AND ol.ol_delivery_d < '2017-01-01 00:00:00.000000'
GROUP BY o.o_ol_cnt
ORDER BY o.o_ol_cnt;

--Q13
SELECT c_orders.c_count, count(*) AS custdist
FROM
  (
    SELECT c.c_id, count(o.o_id) AS c_count
    FROM customer c
      LEFT OUTER JOIN orders o ON (
        c.c_w_id = o.o_w_id
        AND c.c_d_id = o.o_d_id
        AND c.c_id = o.o_c_id
        AND o.o_carrier_id > 8
      )
    GROUP BY c.c_id
  ) AS c_orders
GROUP BY c_orders.c_count
ORDER BY custdist DESC, c_orders.c_count DESC;

--Q14
SELECT
  100.00 * sum(
    CASE WHEN i.i_data LIKE 'pr%' THEN ol.ol_amount ELSE 0 END
  ) / (
    1 + sum(ol.ol_amount)
  ) AS promo_revenue
FROM orders o, o.o_orderline ol, item i
WHERE
  ol.ol_i_id = i.i_id
  AND ol.ol_delivery_d >= '2017-09-01 00:00:00.000000'
  AND ol.ol_delivery_d < '2017-10-01 00:00:00.000000';

--Q15
WITH revenue AS (
  SELECT
    s.s_w_id * s.s_i_id % 10000 AS supplier_no,
    sum(ol.ol_amount) AS total_revenue
  FROM orders o, o.o_orderline ol, stock s
  WHERE
    ol.ol_i_id = s.s_i_id
    AND ol.ol_supply_w_id = s.s_w_id
    AND ol.ol_delivery_d >= '2018-01-01 00:00:00.000000'
    AND ol.ol_delivery_d < '2018-04-01 00:00:00.000000'
  GROUP BY s.s_w_id * s.s_i_id % 10000
)
SELECT su.su_suppkey, su.su_name, su.su_address, su.su_phone, r.total_revenue
FROM supplier su, revenue r
WHERE
  su.su_suppkey = r.supplier_no
  AND r.total_revenue = (
    SELECT VALUE max(r1.total_revenue)
    FROM revenue r1
  )[0]
ORDER BY su.su_suppkey;

--Q16
SELECT
  i.i_name,
  substr1(i.i_data, 1, 3) AS brand,
  i.i_price,
  count(
    DISTINCT (s.s_w_id * s.s_i_id % 10000)
  ) AS supplier_cnt
FROM stock s, item i
WHERE
  i.i_id = s.s_i_id
  AND i.i_data NOT LIKE 'zz%'
  AND (
    s.s_w_id * s.s_i_id % 10000 NOT IN (
      SELECT VALUE su.su_suppkey
      FROM supplier su
      WHERE su.su_comment LIKE '%Customer%Complaints%'
    )
  )
GROUP BY i.i_name, substr1(i.i_data, 1, 3), i.i_price
ORDER BY supplier_cnt DESC;

--Q17
SELECT sum(ol.ol_amount) / 2.0 AS avg_yearly
FROM
  orders o,
  o.o_orderline ol,
  (
    SELECT i.i_id, avg(ol1.ol_quantity) AS a
    FROM item i, orders o1, o1.o_orderline ol1
    WHERE
      i.i_data LIKE '%b'
      AND ol1.ol_i_id = i.i_id
    GROUP BY i.i_id
  ) t
WHERE
  ol.ol_i_id = t.i_id
  AND ol.ol_quantity < t.a;

--Q18
SELECT c.c_name.c_last, c.c_id o_id, o.o_entry_d, o.o_ol_cnt, sum(ol.ol_amount)
FROM customer c, orders o, o.o_orderline ol
WHERE
  c.c_id = o.o_c_id
  AND c.c_w_id = o.o_w_id
  AND c.c_d_id = o.o_d_id
GROUP BY
  o.o_id,
  o.o_w_id,
  o.o_d_id,
  c.c_id,
  c.c_name.c_last,
  o.o_entry_d,
  o.o_ol_cnt
HAVING sum(ol.ol_amount) > 200
ORDER BY sum(ol.ol_amount) DESC, o.o_entry_d
LIMIT 100;

--Q19
SELECT sum(ol.ol_amount) AS revenue
FROM orders o, o.o_orderline ol, item i
WHERE
  (
    (
      i.i_data LIKE '%h'
      AND ol.ol_quantity >= 7
      AND ol.ol_quantity <= 17
      AND i.i_price BETWEEN 1 AND 5
      AND o.o_w_id IN [37, 29, 70]
    )
    OR (
      i.i_data LIKE '%t'
      AND ol.ol_quantity >= 16
      AND ol.ol_quantity <= 26
      AND i.i_price BETWEEN 1 AND 10
      AND o.o_w_id IN [78, 17, 6]
    )
    OR (
      i.i_data LIKE '%m'
      AND ol.ol_quantity >= 24
      AND ol.ol_quantity <= 34
      AND i.i_price BETWEEN 1 AND 15
      AND o.o_w_id IN [91, 95, 15]
    )
  )
  AND ol.ol_i_id = i.i_id
  AND i.i_price BETWEEN 1 AND 15;

--Q20
SELECT su.su_name, su.su_address
FROM supplier su, nation n
WHERE
  su.su_suppkey IN (
    SELECT VALUE s.s_i_id * s.s_w_id % 10000
    FROM stock s, orders o, o.o_orderline ol
    WHERE
      s.s_i_id IN (
        SELECT VALUE i.i_id
        FROM item i
        WHERE i.i_data LIKE 'co%'
      )
      AND ol.ol_i_id = s.s_i_id
      AND ol.ol_delivery_d >= '2016-01-01 12:00:00'
      AND ol.ol_delivery_d < '2017-01-01 12:00:00'
    GROUP BY s.s_i_id, s.s_w_id, s.s_quantity
    HAVING 20 * s.s_quantity > sum(ol.ol_quantity)
  )
  AND su.su_nationkey = n.n_nationkey
  AND n.n_name = 'Germany'
ORDER BY su.su_name;

--Q21
SELECT z.su_name, count (*) AS numwait
FROM
  (
    SELECT x.su_name
    FROM
      (
        SELECT
          o1.o_id,
          o1.o_w_id,
          o1.o_d_id,
          ol1.ol_delivery_d,
          n.n_nationkey,
          su.su_suppkey,
          s.s_w_id,
          s.s_i_id,
          su.su_name
        FROM nation n, supplier su, stock s, orders o1, o1.o_orderline ol1
        WHERE
          o1.o_w_id = s.s_w_id
          AND ol1.ol_i_id = s.s_i_id
          AND s.s_w_id * s.s_i_id % 10000 = su.su_suppkey
          AND datetime(ol1.ol_delivery_d) > datetime(o1.o_entry_d) + duration("P150D")
          AND o1.o_entry_d BETWEEN '2017-12-01 00:00:00' AND '2017-12-31 00:00:00'
          AND su.su_nationkey = n.n_nationkey
          AND n.n_name = 'Peru'
      ) x
      LEFT OUTER JOIN (
        SELECT o2.o_id, o2.o_w_id, o2.o_d_id, ol2.ol_delivery_d
        FROM orders o2, o2.o_orderline ol2
        WHERE
          o2.o_entry_d BETWEEN '2017-12-01 00:00:00' AND '2017-12-31 00:00:00'
      ) y ON y.o_id = x.o_id
      AND y.o_w_id = x.o_w_id
      AND y.o_d_id = x.o_d_id
      AND y.ol_delivery_d > x.ol_delivery_d
    GROUP BY
      x.o_w_id,
      x.o_d_id,
      x.o_id,
      x.n_nationkey,
      x.su_suppkey,
      x.s_w_id,
      x.s_i_id,
      x.su_name
    HAVING count (y.o_id) = 0
  ) z
GROUP BY z.su_name
LIMIT 100;

--Q22
SELECT
  substr1(ca.c_state, 1, 1) AS country,
  count(*) AS numcust,
  sum(c.c_balance) AS totacctbal
FROM customer c, c.c_addresses ca, c.c_phones cp
WHERE
  substr1(cp.c_phone_number, 1, 1) IN ['1', '2', '3', '4', '5', '6', '7']
  AND ca.c_address_kind = 'shipping'
  AND cp.c_phone_kind = 'contact'
  AND c.c_balance > (
    SELECT VALUE avg(c1.c_balance)
    FROM customer c1, c1.c_phones cp1
    WHERE
      c1.c_balance > 0.00
      AND cp1.c_phone_kind = 'contact'
      AND substr1(cp1.c_phone_number, 1, 1) IN ['1', '2', '3', '4', '5', '6', '7']
  )[0]
  AND NOT EXISTS (
    SELECT VALUE 1
    FROM orders o
    WHERE
      o.o_c_id = c.c_id
      AND o.o_w_id = c.c_w_id
      AND o.o_d_id = c.c_d_id
      AND o.o_entry_d BETWEEN '2013-12-01 00:00:00' AND '2013-12-31 00:00:00'
  )
GROUP BY substr1(ca.c_state, 1, 1)
ORDER BY substr1(ca.c_state, 1, 1);
