-- bench-pgbench.sql
--
-- Analytical queries against pgbench schema for benchmarking
-- streambed (DuckDB/Iceberg) vs Postgres.
--
-- Usage:
--   # Against Postgres directly
--   psql -h localhost -p 5432 -U postgres -f scripts/bench-pgbench.sql
--
--   # Against streambed query server
--   psql -h localhost -p 5433 -U postgres -f scripts/bench-pgbench.sql
--
-- Prerequisites:
--   pgbench -i -s 10 -h localhost -U postgres postgres
--   pgbench -T 60 -h localhost -U postgres postgres
--   streambed sync ... (wait for catchup)

\timing on

-- ===================================================================
-- Q1: COUNT accounts
-- Simple full-table scan.
-- ===================================================================
SELECT count(*) FROM pgbench_accounts;

-- ===================================================================
-- Q2: SUM all balances
-- ===================================================================
SELECT sum(abalance) FROM pgbench_accounts;

-- ===================================================================
-- Q3: Balance histogram
-- CASE-based bucketing with aggregation.
-- ===================================================================
SELECT
    CASE
        WHEN abalance < -50000 THEN 'very_negative'
        WHEN abalance < 0      THEN 'negative'
        WHEN abalance < 50000  THEN 'positive'
        ELSE 'very_positive'
    END AS bucket,
    count(*) AS cnt,
    avg(abalance) AS avg_bal
FROM pgbench_accounts
GROUP BY 1 ORDER BY 1;

-- ===================================================================
-- Q4: History delta stats
-- Aggregation over the largest table.
-- ===================================================================
SELECT
    count(*) AS txns,
    sum(delta) AS total_delta,
    avg(delta) AS avg_delta,
    min(delta) AS min_delta,
    max(delta) AS max_delta
FROM pgbench_history;

-- ===================================================================
-- Q5: Top 10 accounts by transaction count
-- GROUP BY + ORDER BY on history.
-- ===================================================================
SELECT aid, count(*) AS txn_count, sum(delta) AS net_delta
FROM pgbench_history
GROUP BY aid
ORDER BY txn_count DESC
LIMIT 10;

-- ===================================================================
-- Q6: Branch totals (two-table join)
-- Join accounts to branches, aggregate per branch.
-- ===================================================================
SELECT b.bid, b.bbalance,
    count(a.aid) AS num_accounts,
    sum(a.abalance) AS total_abalance,
    avg(a.abalance) AS avg_abalance
FROM pgbench_branches b
JOIN pgbench_accounts a ON a.bid = b.bid
GROUP BY b.bid, b.bbalance
ORDER BY b.bid;

-- ===================================================================
-- Q7: History per branch
-- Aggregate history grouped by branch.
-- ===================================================================
SELECT h.bid,
    count(*) AS txn_count,
    sum(h.delta) AS total_delta,
    avg(h.delta) AS avg_delta
FROM pgbench_history h
GROUP BY h.bid
ORDER BY total_delta DESC;

-- ===================================================================
-- Q8: Teller activity with branch (three-table join)
-- ===================================================================
SELECT t.tid, b.bid, b.bbalance,
    count(h.aid) AS txn_count,
    sum(h.delta) AS total_delta
FROM pgbench_tellers t
JOIN pgbench_branches b ON b.bid = t.bid
JOIN pgbench_history h ON h.tid = t.tid
GROUP BY t.tid, b.bid, b.bbalance
ORDER BY txn_count DESC
LIMIT 20;

-- ===================================================================
-- Q9: Full 4-table join — account activity report (the monster)
-- Joins all 4 pgbench tables with conditional aggregates.
-- ===================================================================
SELECT
    b.bid AS branch_id,
    b.bbalance AS branch_balance,
    count(DISTINCT a.aid) AS active_accounts,
    count(h.delta) AS total_txns,
    sum(h.delta) AS net_flow,
    avg(h.delta) AS avg_txn_size,
    sum(CASE WHEN h.delta > 0 THEN h.delta ELSE 0 END) AS credits,
    sum(CASE WHEN h.delta < 0 THEN h.delta ELSE 0 END) AS debits,
    count(DISTINCT h.tid) AS active_tellers
FROM pgbench_branches b
JOIN pgbench_accounts a ON a.bid = b.bid
JOIN pgbench_history h ON h.aid = a.aid
JOIN pgbench_tellers t ON t.tid = h.tid AND t.bid = b.bid
GROUP BY b.bid, b.bbalance
ORDER BY total_txns DESC;

-- ===================================================================
-- Q10: Accounts with above-average transaction volume
-- Subquery with nested aggregation.
-- ===================================================================
SELECT aid, txn_count, net_delta
FROM (
    SELECT aid,
        count(*) AS txn_count,
        sum(delta) AS net_delta
    FROM pgbench_history
    GROUP BY aid
) sub
WHERE txn_count > (
    SELECT avg(c) FROM (
        SELECT count(*) AS c FROM pgbench_history GROUP BY aid
    ) avg_sub
)
ORDER BY txn_count DESC
LIMIT 20;

-- ===================================================================
-- Q11: Branch P50/P95 account balance
-- percentile_cont — heavy sort-based aggregation.
-- ===================================================================
SELECT bid,
    count(*) AS num_accounts,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY abalance) AS p50_balance,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY abalance) AS p95_balance,
    min(abalance) AS min_balance,
    max(abalance) AS max_balance
FROM pgbench_accounts
GROUP BY bid
ORDER BY bid;

-- ===================================================================
-- Q12: Correlated subquery — each account's txn count vs branch avg
-- Inner query re-executes per outer row. Very slow on large data.
-- ===================================================================
SELECT a.aid, a.bid, a.abalance,
    (SELECT count(*) FROM pgbench_history h WHERE h.aid = a.aid) AS my_txns,
    (SELECT avg(cnt) FROM (
        SELECT count(*) AS cnt FROM pgbench_history h2
        JOIN pgbench_accounts a2 ON a2.aid = h2.aid
        WHERE a2.bid = a.bid GROUP BY h2.aid
    ) sub) AS branch_avg_txns
FROM pgbench_accounts a
WHERE a.abalance > 0
ORDER BY my_txns DESC, a.aid ASC
LIMIT 500;

-- ===================================================================
-- Q13: Self-join on history — accounts with repeated teller visits
-- Quadratic potential on large history tables.
-- ===================================================================
SELECT h1.aid, h1.tid,
    count(*) AS repeat_visits,
    sum(h1.delta) AS cumulative_delta,
    min(h1.mtime) AS first_visit,
    max(h1.mtime) AS last_visit
FROM pgbench_history h1
JOIN pgbench_history h2
    ON h1.aid = h2.aid AND h1.tid = h2.tid AND h1.mtime < h2.mtime
GROUP BY h1.aid, h1.tid
HAVING count(*) > 3
ORDER BY repeat_visits DESC
LIMIT 100;

-- ===================================================================
-- Q14: 4-table join with window functions — running balance per account
-- Window over full history with lag().
-- ===================================================================
SELECT * FROM (
    SELECT
        b.bid,
        a.aid,
        h.delta,
        h.mtime,
        t.tid,
        sum(h.delta) OVER (PARTITION BY a.aid ORDER BY h.mtime) AS running_balance,
        row_number() OVER (PARTITION BY a.aid ORDER BY h.mtime) AS txn_seq,
        lag(h.delta) OVER (PARTITION BY a.aid ORDER BY h.mtime) AS prev_delta
    FROM pgbench_history h
    JOIN pgbench_accounts a ON a.aid = h.aid
    JOIN pgbench_branches b ON b.bid = a.bid
    JOIN pgbench_tellers t ON t.tid = h.tid
) ranked
WHERE txn_seq <= 5
ORDER BY bid, aid, txn_seq;

-- ===================================================================
-- Q15: Branch-to-branch comparison matrix
-- Cross join on branches with two full history scans.
-- ===================================================================
SELECT
    b1.bid AS branch_a,
    b2.bid AS branch_b,
    s1.total_txns AS a_txns,
    s2.total_txns AS b_txns,
    s1.net_flow - s2.net_flow AS flow_diff,
    s1.avg_balance - s2.avg_balance AS balance_diff
FROM pgbench_branches b1
CROSS JOIN pgbench_branches b2
JOIN (
    SELECT a.bid,
        count(h.delta) AS total_txns,
        sum(h.delta) AS net_flow,
        avg(a.abalance) AS avg_balance
    FROM pgbench_accounts a
    JOIN pgbench_history h ON h.aid = a.aid
    GROUP BY a.bid
) s1 ON s1.bid = b1.bid
JOIN (
    SELECT a.bid,
        count(h.delta) AS total_txns,
        sum(h.delta) AS net_flow,
        avg(a.abalance) AS avg_balance
    FROM pgbench_accounts a
    JOIN pgbench_history h ON h.aid = a.aid
    GROUP BY a.bid
) s2 ON s2.bid = b2.bid
WHERE b1.bid < b2.bid
ORDER BY abs(s1.net_flow - s2.net_flow) DESC;

-- ===================================================================
-- Q16: Full history replay with running stats and pattern detection
-- Multiple window functions over entire history table. The monster.
-- ===================================================================
SELECT
    aid,
    delta,
    mtime,
    sum(delta) OVER w AS running_total,
    avg(delta) OVER w AS running_avg,
    min(delta) OVER w AS running_min,
    max(delta) OVER w AS running_max,
    count(*) OVER w AS txn_number,
    delta - lag(delta) OVER (PARTITION BY aid ORDER BY mtime) AS delta_change,
    CASE
        WHEN delta > 0 AND lag(delta) OVER (PARTITION BY aid ORDER BY mtime) < 0 THEN 'reversal_up'
        WHEN delta < 0 AND lag(delta) OVER (PARTITION BY aid ORDER BY mtime) > 0 THEN 'reversal_down'
        ELSE 'continuation'
    END AS pattern
FROM pgbench_history
WINDOW w AS (PARTITION BY aid ORDER BY mtime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
ORDER BY aid, mtime;

\timing off
