--Q01
SELECT
  ol_number,
  sum(o.o_orderline.ol_quantity) AS sum_qty,
  sum(o.o_orderline.ol_amount) AS sum_amount,
  avg(o.o_orderline.ol_quantity) AS avg_qty,
  avg(o.o_orderline.ol_amount) AS avg_amount,
  count(*) AS count_order
FROM UNWIND (orders AS o WITH PATH => o_orderline)
WHERE o.o_orderline.ol_delivery_d > '2014-07-01 00:00:00'
GROUP BY o.o_orderline.ol_number AS ol_number
ORDER BY ol_number;

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
      mod(s1.s_w_id * s1.s_i_id, 10000) = su1.su_suppkey
      AND su1.su_nationkey = n1.n_nationkey
      AND n1.n_regionkey = r1.r_regionkey
      AND r1.r_name LIKE 'Europ%'
    GROUP BY s1.s_i_id
  ) m
WHERE
  i.i_id = s.s_i_id
  AND mod(s.s_w_id * s.s_i_id, 10000) = su.su_suppkey
  AND su.su_nationkey = n.n_nationkey
  AND n.n_regionkey = r.r_regionkey
  AND i.i_data LIKE '%b'
  AND r.r_name LIKE 'Europ%'
  AND i.i_id = m.m_i_id
  AND s.s_quantity = m.m_s_quantity
ORDER BY n.n_name, su.su_name, i.i_id
LIMIT 100;

--Q03
SELECT
  o.o_id,
  o.o_w_id,
  o.o_d_id,
  sum(o.o_orderline.ol_amount) AS revenue,
  o.o_entry_d
FROM customer c, neworder no, UNWIND (orders AS o WITH PATH => o_orderline)
WHERE
  c.c_state LIKE 'a%'
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
SELECT ol_cnt, count(*) AS order_count
FROM orders o
WHERE
  o.o_entry_d >= '2015-07-01 00:00:00.000000'
  AND o.o_entry_d < '2015-10-01 00:00:00.000000'
  AND EXISTS (
    SELECT 1
    FROM UNWIND (orders AS o WITH PATH => o_orderline)
    WHERE
      o.o_orderline.ol_delivery_d >= cast(
        dateadd(week, 1, cast(o.o_entry_d AS timestamp)) AS string
      )
  )
GROUP BY o.o_ol_cnt AS ol_cnt
ORDER BY ol_cnt;

--Q05
SELECT nname, round(sum(o.o_orderline.ol_amount), 2) AS revenue
FROM
  customer c,
  UNWIND (orders AS o WITH PATH => o_orderline),
  stock s,
  supplier su,
  nation n,
  region r
WHERE
  c.c_id = o.o_c_id
  AND c.c_w_id = o.o_w_id
  AND c.c_d_id = o.o_d_id
  AND o.o_w_id = s.s_w_id
  AND o.o_orderline.ol_i_id = s.s_i_id
  AND mod(s.s_w_id * s.s_i_id, 10000) = su.su_suppkey
  AND position(
    substring(c.c_state, 0, 1) IN '##########%%%%%%%%%%##########%%%%%%%%%%########0123456789#######ABCDEFGHIJKLMNOPQRSTUVWXYZ%%%%%%abcdefghijklmnopqrstuvwxyz'
  ) = su.su_nationkey
  AND position(
    substring(c.c_state, 0, 1) IN '##########%%%%%%%%%%##########%%%%%%%%%%########0123456789#######ABCDEFGHIJKLMNOPQRSTUVWXYZ%%%%%%abcdefghijklmnopqrstuvwxyz'
  ) = n.n_nationkey
  AND su.su_nationkey = n.n_nationkey
  AND n.n_regionkey = r.r_regionkey
  AND r.r_name = 'Asia'
  AND o.o_entry_d >= '2016-01-01 00:00:00.000000'
  AND o.o_entry_d < '2017-01-01 00:00:00.000000'
GROUP BY n.n_name AS nname
ORDER BY revenue DESC;

--Q06
SELECT sum(o.o_orderline.ol_amount) AS revenue
FROM UNWIND (orders AS o WITH PATH => o_orderline)
WHERE
  o.o_orderline.ol_delivery_d >= '2016-01-01 00:00:00.000000'
  AND o.o_orderline.ol_delivery_d < '2017-01-01 00:00:00.000000'
  AND o.o_orderline.ol_amount > 600;

--Q07
SELECT
  supp_nation,
  cust_nation,
  l_year,
  round(sum(o.o_orderline.ol_amount), 2) AS revenue
FROM
  supplier su,
  stock s,
  UNWIND (orders AS o WITH PATH => o_orderline),
  customer c,
  nation n1,
  nation n2
WHERE
  o.o_orderline.ol_supply_w_id = s.s_w_id
  AND o.o_orderline.ol_i_id = s.s_i_id
  AND mod(s.s_w_id * s.s_i_id, 10000) = su.su_suppkey
  AND c.c_id = o.o_c_id
  AND c.c_w_id = o.o_w_id
  AND c.c_d_id = o.o_d_id
  AND su.su_nationkey = n1.n_nationkey
  AND position(
    substring(c.c_state, 0, 1) IN '##########%%%%%%%%%%##########%%%%%%%%%%########0123456789#######ABCDEFGHIJKLMNOPQRSTUVWXYZ%%%%%%abcdefghijklmnopqrstuvwxyz'
  ) = n2.n_nationkey
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
  AND o.o_orderline.ol_delivery_d BETWEEN '2017-01-01 00:00:00.000000' AND '2018-12-31 00:00:00.000000'
GROUP BY
  su.su_nationkey AS supp_nation,
  substring(c.c_state, 0, 1) AS cust_nation,
  extract(year FROM cast(o.o_entry_d AS timestamp)) AS l_year
ORDER BY supp_nation, cust_nation, l_year;

--Q08
SELECT
  l_year,
  round(
    (
      sum(
        CASE WHEN n2.n_name = 'Germany' THEN o.o_orderline.ol_amount ELSE 0 END
      ) / sum(o.o_orderline.ol_amount)
    ),
    2
  ) AS mkt_share
FROM
  item i,
  supplier su,
  stock s,
  UNWIND (orders AS o WITH PATH => o_orderline),
  customer c,
  nation n1,
  nation n2,
  region r
WHERE
  i.i_id = s.s_i_id
  AND o.o_orderline.ol_i_id = s.s_i_id
  AND o.o_orderline.ol_supply_w_id = s.s_w_id
  AND mod(s.s_w_id * s.s_i_id, 10000) = su.su_suppkey
  AND c.c_id = o.o_c_id
  AND c.c_w_id = o.o_w_id
  AND c.c_d_id = o.o_d_id
  AND n1.n_nationkey = position(
    substring(c.c_state, 0, 1) IN '##########%%%%%%%%%%##########%%%%%%%%%%########0123456789#######ABCDEFGHIJKLMNOPQRSTUVWXYZ%%%%%%abcdefghijklmnopqrstuvwxyz'
  )
  AND n1.n_regionkey = r.r_regionkey
  AND o.o_orderline.ol_i_id < 1000
  AND r.r_name = 'Europe'
  AND su.su_nationkey = n2.n_nationkey
  AND o.o_entry_d BETWEEN '2017-01-01 00:00:00.000000' AND '2018-12-31 00:00:00.000000'
  AND i.i_data LIKE '%b'
  AND i.i_id = o.o_orderline.ol_i_id
GROUP BY extract(year FROM cast(o.o_entry_d AS timestamp)) AS l_year
ORDER BY l_year;

--Q09
SELECT n_name, l_year, sum(o.o_orderline.ol_amount) AS sum_profit
FROM
  item i,
  stock s,
  supplier su,
  UNWIND (orders AS o WITH PATH => o_orderline),
  nation n
WHERE
  o.o_orderline.ol_i_id = s.s_i_id
  AND o.o_orderline.ol_supply_w_id = s.s_w_id
  AND mod(s.s_w_id * s.s_i_id, 10000) = su.su_suppkey
  AND o.o_orderline.ol_i_id = i.i_id
  AND su.su_nationkey = n.n_nationkey
  AND i.i_data LIKE '%bb'
GROUP BY
  n.n_name AS n_name,
  extract(year FROM cast(o.o_entry_d AS timestamp)) AS l_year
ORDER BY n_name, l_year DESC;

--Q10
SELECT cid, clast, sum(o.o_orderline.ol_amount) AS revenue, ccity, cphone, nname
FROM customer c, UNWIND (orders AS o WITH PATH => o_orderline), nation n
WHERE
  c.c_id = o.o_c_id
  AND c.c_w_id = o.o_w_id
  AND c.c_d_id = o.o_d_id
  AND o.o_entry_d >= '2015-10-01 00:00:00.000000'
  AND o.o_entry_d < '2016-01-01 00:00:00.000000'
  AND n.n_nationkey = position(
    substring(c.c_state, 0, 1) IN '##########%%%%%%%%%%##########%%%%%%%%%%########0123456789#######ABCDEFGHIJKLMNOPQRSTUVWXYZ%%%%%%abcdefghijklmnopqrstuvwxyz'
  )
GROUP BY
  c.c_id AS cid,
  c.c_last AS clast,
  c.c_city AS ccity,
  c.c_phone AS cphone,
  n.n_name AS nname
ORDER BY revenue DESC
LIMIT 20;

--Q11
SELECT s.s_i_id, sum(s.s_order_cnt) AS ordercount
FROM stock s, supplier su, nation n
WHERE
  mod(s.s_w_id * s.s_i_id, 10000) = su.su_suppkey
  AND su.su_nationkey = n.n_nationkey
  AND n.n_name = 'Germany'
GROUP BY s.s_i_id
HAVING
  sum(s.s_order_cnt) > (
    SELECT sum(s1.s_order_cnt) * 0.00005
    FROM stock s1, supplier su1, nation n1
    WHERE
      mod(s1.s_w_id * s1.s_i_id, 10000) = su1.su_suppkey
      AND su1.su_nationkey = n1.n_nationkey
      AND n1.n_name = 'Germany'
  )
ORDER BY ordercount DESC;

--Q12
SELECT
  ol_cnt,
  sum(
    CASE WHEN o.o_carrier_id = 1
    OR o.o_carrier_id = 2 THEN 1 ELSE 0 END
  ) AS high_line_count,
  sum(
    CASE WHEN o.o_carrier_id <> 1
    AND o.o_carrier_id <> 2 THEN 1 ELSE 0 END
  ) AS low_line_count
FROM UNWIND (orders AS o WITH PATH => o_orderline)
WHERE
  o.o_entry_d <= o.o_orderline.ol_delivery_d
  AND o.o_orderline.ol_delivery_d >= '2016-01-01 00:00:00.000000'
  AND o.o_orderline.ol_delivery_d < '2017-01-01 00:00:00.000000'
GROUP BY o.o_ol_cnt AS ol_cnt
ORDER BY ol_cnt;

--Q13
SELECT c_count, count(*) AS custdist
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
GROUP BY c_orders.c_count AS c_count
ORDER BY custdist DESC, c_count DESC;

--Q14
SELECT
  100.00 * sum(
    CASE WHEN i.i_data LIKE 'pr%' THEN o.o_orderline.ol_amount ELSE 0 END
  ) / (
    1 + sum(o.o_orderline.ol_amount)
  ) AS promo_revenue
FROM UNWIND (orders AS o WITH PATH => o_orderline), item i
WHERE
  o.o_orderline.ol_i_id = i.i_id
  AND o.o_orderline.ol_delivery_d >= '2017-09-01 00:00:00.000000'
  AND o.o_orderline.ol_delivery_d < '2017-10-01 00:00:00.000000';

--Q15
SELECT su.su_suppkey, su.su_name, su.su_address, su.su_phone, r.total_revenue
FROM
  supplier su,
  (
    SELECT supplier_no, sum(o.o_orderline.ol_amount) AS total_revenue
    FROM UNWIND (orders AS o WITH PATH => o_orderline), stock s
    WHERE
      o.o_orderline.ol_i_id = s.s_i_id
      AND o.o_orderline.ol_supply_w_id = s.s_w_id
      AND o.o_orderline.ol_delivery_d >= '2018-01-01 00:00:00.000000'
      AND o.o_orderline.ol_delivery_d < '2018-04-01 00:00:00.000000'
    GROUP BY mod(s.s_w_id * s.s_i_id, 10000) AS supplier_no
  ) AS r
WHERE
  su.su_suppkey = r.supplier_no
  AND r.total_revenue = (
    SELECT max(r1.total_revenue)
    FROM
      (
        SELECT supplier_no, sum(o.o_orderline.ol_amount) AS total_revenue
        FROM UNWIND (orders AS o WITH PATH => o_orderline), stock s
        WHERE
          o.o_orderline.ol_i_id = s.s_i_id
          AND o.o_orderline.ol_supply_w_id = s.s_w_id
          AND o.o_orderline.ol_delivery_d >= '2018-01-01 00:00:00.000000'
          AND o.o_orderline.ol_delivery_d < '2018-04-01 00:00:00.000000'
        GROUP BY mod(s.s_w_id * s.s_i_id, 10000) AS supplier_no
      ) AS r1
  )
ORDER BY su.su_suppkey;

--Q16
SELECT
  iname,
  brand,
  iprice,
  count(DISTINCT mod( (s.s_w_id * s.s_i_id), 10000)) AS supplier_cnt
FROM stock s, item i
WHERE
  i.i_id = s.s_i_id
  AND i.i_data NOT LIKE 'zz%'
  AND (
    mod((s.s_w_id * s.s_i_id), 10000) NOT IN (
      SELECT su.su_suppkey
      FROM supplier su
      WHERE su.su_comment LIKE '%Customer%Complaints%'
    )
  )
GROUP BY
  i.i_name AS iname,
  substring(i.i_data, 0, 3) AS brand,
  i.i_price AS iprice
ORDER BY supplier_cnt DESC;

--Q17
SELECT sum(o.o_orderline.ol_amount) / 2.0 AS avg_yearly
FROM
  UNWIND (orders AS o WITH PATH => o_orderline),
  (
    SELECT iid, avg(o1.o_orderline.ol_quantity) AS a
    FROM item i, UNWIND (orders AS o1 WITH PATH => o_orderline)
    WHERE
      i.i_data LIKE '%b'
      AND o1.o_orderline.ol_i_id = i.i_id
    GROUP BY i.i_id AS iid
  ) t
WHERE
  o.o_orderline.ol_i_id = t.iid
  AND o.o_orderline.ol_quantity < t.a;

--Q18
SELECT
  clast,
  c.c_id,
  o.o_id,
  o.o_entry_d,
  o.o_ol_cnt,
  sum(o.o_orderline.ol_amount) AS ol_sum
FROM customer c, UNWIND (orders AS o WITH PATH => o_orderline)
WHERE
  c.c_id = o.o_c_id
  AND c.c_w_id = o.o_w_id
  AND c.c_d_id = o.o_d_id
GROUP BY
  o.o_id,
  o.o_w_id,
  o.o_d_id,
  c.c_id,
  c.c_name.c_last AS clast,
  o.o_entry_d,
  o.o_ol_cnt
HAVING sum(o.o_orderline.ol_amount) > 200
ORDER BY ol_sum DESC, o.o_entry_d
LIMIT 100;

--Q19
SELECT sum(o.o_orderline.ol_amount) AS revenue
FROM UNWIND (orders AS o WITH PATH => o_orderline), item i
WHERE
  (
    (
      i.i_data LIKE '%h'
      AND o.o_orderline.ol_quantity >= 7
      AND o.o_orderline.ol_quantity <= 17
      AND i.i_price BETWEEN 1 AND 5
      AND o.o_w_id IN (37, 29, 70)
    )
    OR (
      i.i_data LIKE '%t'
      AND o.o_orderline.ol_quantity >= 16
      AND o.o_orderline.ol_quantity <= 26
      AND i.i_price BETWEEN 1 AND 10
      AND o.o_w_id IN (78, 17, 6)
    )
    OR (
      i.i_data LIKE '%m'
      AND o.o_orderline.ol_quantity >= 24
      AND o.o_orderline.ol_quantity <= 34
      AND i.i_price BETWEEN 1 AND 15
      AND o.o_w_id IN (91, 95, 15)
    )
  )
  AND o.o_orderline.ol_i_id = i.i_id
  AND i.i_price BETWEEN 1 AND 15;

--Q20
SELECT su.su_name, su.su_address
FROM supplier su, nation n
WHERE
  su.su_suppkey IN (
    SELECT mod(s.s_i_id * s.s_w_id, 10000)
    FROM stock s, UNWIND (orders AS o WITH PATH => o_orderline)
    WHERE
      s.s_i_id IN (
        SELECT i.i_id
        FROM item i
        WHERE i.i_data LIKE 'co%'
      )
      AND o.o_orderline.ol_i_id = s.s_i_id
      AND o.o_orderline.ol_delivery_d >= '2016-01-01 12:00:00'
      AND o.o_orderline.ol_delivery_d < '2017-01-01 12:00:00'
    GROUP BY s.s_i_id, s.s_w_id, s.s_quantity
    HAVING 20 * s.s_quantity > sum(o.o_orderline.ol_quantity)
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
          o1.o_orderline.ol_delivery_d,
          n.n_nationkey,
          su.su_suppkey,
          s.s_w_id,
          s.s_i_id,
          su.su_name
        FROM
          nation n,
          supplier su,
          stock s,
          UNWIND (orders AS o1 WITH PATH => o_orderline)
        WHERE
          o1.o_w_id = s.s_w_id
          AND o1.o_orderline.ol_i_id = s.s_i_id
          AND mod(s.s_w_id * s.s_i_id, 10000) = su.su_suppkey
          AND o1.o_orderline.ol_delivery_d > cast(
            dateadd(day, 150, cast(o1.o_entry_d AS timestamp)) AS string
          )
          AND o1.o_entry_d BETWEEN '2017-12-01 00:00:00' AND '2017-12-31 00:00:00'
          AND su.su_nationkey = n.n_nationkey
          AND n.n_name = 'Peru'
      ) x
      LEFT OUTER JOIN (
        SELECT o2.o_id, o2.o_w_id, o2.o_d_id, o2.o_orderline.ol_delivery_d
        FROM UNWIND (orders AS o2 WITH PATH => o_orderline)
        WHERE o2.o_entry_d BETWEEN '2017-12-01 00:00:00' AND '2017-12-31 00:00:00'
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
SELECT country, count(*) AS numcust, sum(c.c_balance) AS totacctbal
FROM customer c
WHERE
  substring(c.c_phone, 0, 1) IN ('1', '2', '3', '4', '5', '6', '7')
  AND c.c_balance > (
    SELECT avg(c1.c_balance)
    FROM customer c1
    WHERE
      c1.c_balance > 0.00
      AND substring(c1.c_phone, 0, 1) IN ('1', '2', '3', '4', '5', '6', '7')
  )
  AND NOT EXISTS (
    SELECT 1
    FROM orders o
    WHERE
      o.o_c_id = c.c_id
      AND o.o_w_id = c.c_w_id
      AND o.o_d_id = c.c_d_id
      AND o.o_entry_d BETWEEN '2013-12-01 00:00:00' AND '2013-12-31 00:00:00'
  )
GROUP BY substring(c.c_state, 0, 1) AS country
ORDER BY country;
