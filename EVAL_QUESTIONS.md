# TPC-H Eval Questions

The agent's eval set is the 22 standard [TPC-H](https://www.tpc.org/tpch/) decision-support queries. Each one has a known correct answer from DuckDB, so grading means checking the agent's numbers, not its SQL. The questions and reference SQL are in [TPCH_QUESTIONS.md](TPCH_QUESTIONS.md), which the DH-1 TPC-H set is also built from. This file says how to set up, run, and grade them.

This replaces the earlier events-table eval (37 questions on a DuckLake table), whose question file and generator script have been removed.

1. **Setup**: build the data once and check the reference answers.
2. **Grading**: the three scores and what they mean.
3. **The questions**: 22 questions in five categories, each with its expected result.
4. **Running the batch**: one run per question, with the traces loaded for scoring.
5. **Trace queries**: SQL for scoring and investigating runs in `agent_traces.duckdb`.

---

## 1. Setup

Build the data once. The default scale factor is 0.1, about 600,000 line items:

```bash
uv run agent_harness.py --tpch --save-db tpch.duckdb --no-run
```

The DuckDB `tpch` extension downloads on first use. `tpch.duckdb` is git-ignored, so each machine builds its own copy.

Every run in this eval uses the file read-only, with no `--table` flag, so the agent has to find the tables itself:

```bash
uv run agent_harness.py --db tpch.duckdb --quiet --prompt "<a question from section 3>"
```

The tables are `customer`, `lineitem`, `nation`, `orders`, `part`, `partsupp`, `region`, and `supplier`.

**All expected values here are for scale factor 0.1.** The DH-1 built-in TPC-H set runs at 0.01, where several answers differ (Q17 is NULL there). Check the scale before you compare numbers.

To check an answer yourself, attach the same file and ask for the reference result:

```sql
INSTALL tpch; LOAD tpch;
ATTACH 'tpch.duckdb' AS tpch (READ_ONLY);
USE tpch;
PRAGMA tpch(6);                                                                -- reference result for Q6
SELECT answer FROM tpch_answers() WHERE query_nr = 6 AND scale_factor = 0.1;   -- DuckDB's published answer
```

`tpch_answers()` only has scale factors 0.01, 0.1, and 1.

---

## 2. Grading

Score each run on three things:

| Dimension | 2 | 1 | 0 |
|---|---|---|---|
| **Correct** | Every number matches the expected result: same rows, same values, to the cent or the precision shown. Any SQL that gets there is fine. | Right approach, minor errors (a rounding slip, one row missing, a column mislabeled) | Wrong or invented numbers, or the wrong table |
| **Careful** | States the assumptions the question needs: date windows, what a count counts, how an ambiguous phrase was read | Some of them | Overclaims, or misses a reading that changes the answer |
| **Efficient** | Few iterations, no failed or repeated queries, no truncated dumps | Some waste | Loops, many errors, or hits the iteration limit |

A question **passes** when Correct is 2. Careful breaks ties between runs that pass. Efficient comes from the traces (section 5).

---

## 3. The questions

The categories group questions by what makes them hard, so a weak score in one category points at one kind of SQL.

| Category | Questions | What it tests |
|---|---|---|
| **A. Aggregation and filters** | Q1, Q6, Q12, Q14, Q17 | Grouped sums and averages, with date and value filters |
| **B. Joins and top-N** | Q2, Q3, Q5, Q7, Q9, Q10, Q15 | Multi-table joins, ranking, and a max over a derived table |
| **C. Subqueries** | Q11, Q18, Q20, Q22 | Correlated and nested subqueries, and thresholds taken from another query |
| **D. Existence and set logic** | Q4, Q13, Q16, Q21 | EXISTS, NOT EXISTS, NOT IN, and outer joins |
| **E. Conditional logic** | Q8, Q19 | Ratios over conditions, and OR'd profiles |

| Q | Topic | Tables | Category |
|---|---|---|---|
| Q1 | Pricing summary report | lineitem | A |
| Q2 | Minimum cost supplier | part, supplier, partsupp, nation, region | B |
| Q3 | Shipping priority | customer, orders, lineitem | B |
| Q4 | Order priority checking | orders, lineitem | D |
| Q5 | Local supplier volume | customer, orders, lineitem, supplier, nation, region | B |
| Q6 | Forecasting revenue change | lineitem | A |
| Q7 | Volume shipping | supplier, lineitem, orders, customer, nation | B |
| Q8 | National market share | part, supplier, lineitem, orders, customer, nation, region | E |
| Q9 | Product type profit measure | part, supplier, lineitem, partsupp, orders, nation | B |
| Q10 | Returned item reporting | customer, orders, lineitem, nation | B |
| Q11 | Important stock identification | partsupp, supplier, nation | C |
| Q12 | Shipping modes and order priority | orders, lineitem | A |
| Q13 | Customer distribution | customer, orders | D |
| Q14 | Promotion effect | lineitem, part | A |
| Q15 | Top supplier | supplier, lineitem | B |
| Q16 | Parts/supplier relationship | partsupp, part, supplier | D |
| Q17 | Small-quantity-order revenue | lineitem, part | A |
| Q18 | Large volume customer | customer, orders, lineitem | C |
| Q19 | Discounted revenue | lineitem, part | E |
| Q20 | Potential part promotion | supplier, nation, partsupp, part, lineitem | C |
| Q21 | Suppliers who kept orders waiting | supplier, lineitem, orders, nation | D |
| Q22 | Global sales opportunity | customer, orders | C |

Each question below gives the prompt sent to the agent, the expected result at scale factor 0.1, and what a careful answer says.

### A. Aggregation and filters

**Q1. Pricing summary report**
> For line items shipped on or before 1998-09-02, summarize quantity, base price, discounted price, charge, average quantity/price/discount, and line count by return flag and line status.

- **Expected:** 4 rows, one per (return flag, line status): A/F, N/F, N/O, R/F. A/F has `sum_qty` 3,774,200.00 and `sum_base_price` 5,320,753,880.69.
- **Careful:** says the averages are per line item, not per order, and that the date is the question's fixed cutoff.

**Q6. Forecasting revenue change**
> How much revenue would have been gained in 1994 by eliminating discounts of 5-7% on line items with quantity under 24?

- **Expected:** one value, **11,803,420.25**.
- **Careful:** says it read "5-7%" as inclusive (0.05 to 0.07) and that quantity under 24 means below 24.

**Q12. Shipping modes and order priority**
> For MAIL and SHIP line items received in 1994 that were shipped before their commit date but received after it, how many were on high-priority orders (1-URGENT or 2-HIGH) versus other orders, by ship mode?

- **Expected:** 2 rows. MAIL: 647 high, 945 other. SHIP: 620 high, 943 other.
- **Careful:** says the counts are line items, not orders.

**Q14. Promotion effect**
> What share of September 1995 revenue came from promotional parts?

- **Expected:** one value, **16.28%** (16.283856 as a fraction of 1).
- **Careful:** says what "promotional" means in the data (part type starting with PROMO), and that revenue is extended price times (1 - discount).

**Q17. Small-quantity-order revenue**
> What would the average yearly revenue loss be if Brand#23 MED BOX parts were no longer sold in orders under 20% of their average order quantity?

- **Expected:** one value, **23,512.75**.
- **Careful:** says it divided the total by 7 years (the data spans 1992 to 1998), and why. A total before the division is not the answer.

### B. Joins and top-N

**Q2. Minimum cost supplier**
> For brass parts of size 15 in the EUROPE region, which suppliers offer the lowest supply cost? Show the top 100 by supplier account balance, with the part and supplier details.

- **Expected:** 44 rows. The limit of 100 doesn't bind at this scale. The top row has `s_acctbal` 9828.21 (Supplier#000000647, UNITED KINGDOM).
- **Careful:** says the result has 44 rows and that the limit of 100 is an upper bound, not a count.

**Q3. Shipping priority**
> Which 10 unshipped orders in the BUILDING market segment, placed before 1995-03-15 with items shipping after that date, have the highest potential revenue?

- **Expected:** 10 rows. The top is order 223140 with revenue 355,369.07, order date 1995-03-14, priority 0.
- **Careful:** says revenue is extended price times (1 - discount), with no tax or shipping.

**Q5. Local supplier volume**
> What was the 1994 revenue from line items where customer and supplier are in the same nation, for each nation in the ASIA region?

- **Expected:** 5 rows, highest first: CHINA 7,822,103.00; INDIA 6,376,121.51; JAPAN 6,000,077.22; INDONESIA 5,580,475.40; VIETNAM 4,497,840.55.
- **Careful:** says "same nation" means the customer's and the supplier's nation match, and that revenue is computed the same way as Q3.

**Q7. Volume shipping**
> What was the revenue in 1995 and 1996 from goods shipped between FRANCE and GERMANY (in either direction), by supplier nation, customer nation, and year?

- **Expected:** 4 rows: FRANCE to GERMANY 1995 4,637,235.15; FRANCE to GERMANY 1996 5,224,779.57; GERMANY to FRANCE 1995 6,232,818.70; GERMANY to FRANCE 1996 5,557,312.11.
- **Careful:** says both directions are included as separate rows, not netted.

**Q9. Product type profit measure**
> What was the profit on parts with 'green' in their name, by supplier nation and order year?

- **Expected:** 175 rows, ordered by nation then year, newest first. The first row is ALGERIA 1998 with profit 2,321,785.37.
- **Careful:** says profit is revenue minus supply cost times quantity, and that "green" is a substring match on the part name.

**Q10. Returned item reporting**
> Which 20 customers lost the most revenue to returned items on orders placed in Q4 1993?

- **Expected:** 20 rows. The top is customer 8242 (Customer#000008242, ETHIOPIA) with revenue 622,786.73.
- **Careful:** says what a returned item is (return flag R) and the quarter window it used.

**Q15. Top supplier**
> Which supplier generated the most revenue from shipments in Q1 1996?

- **Expected:** 1 row, supplier 677 (Supplier#000000677) with total revenue 1,614,410.29.
- **Careful:** says whether it checked for ties at the maximum. The reference has none.

### C. Subqueries

**Q11. Important stock identification**
> Which parts in GERMANY make up the most valuable stock (supply cost x available quantity), counting only those above a small fraction of the nation's total?

- **Expected:** 2,541 rows, highest value first. The top is part 12098 with value 16,227,681.21.
- **Careful:** states the fraction it used. The reference uses a fixed 0.0001 of the German total, which is a choice the question leaves open.

**Q18. Large volume customer**
> Which orders have line items totaling more than 300 units? For up to 100 of them, show the customer, order key, order date, total price, and total quantity, largest total price first.

- **Expected:** 5 rows. The top is customer 1639 (Customer#000001639) with order 502886, total price 456,423.88, total quantity 312.00.
- **Careful:** says "more than 300" is strict (300 itself is excluded).

**Q20. Potential part promotion**
> Which CANADA suppliers have an excess of forest-named parts, with stock above half of what was shipped in 1994?

- **Expected:** 9 suppliers, in name order. The first is Supplier#000000157.
- **Careful:** says how it read "forest-named" (name starts with "forest") and the 1994 window for shipments.

**Q22. Global sales opportunity**
> Among customers whose phone number starts with country code 13, 31, 23, 29, 30, 18, or 17 and who have never placed an order, how many have an account balance above the average positive balance of customers in those codes, and what is their total balance, by country code?

- **Expected:** 7 rows, one per code (13, 17, 18, 23, 29, 30, 31). Code 13 has 94 customers with total balance 714,035.05.
- **Careful:** says the average is taken over positive balances of customers with those codes, and that "never placed an order" means no matching row in `orders`.

### D. Existence and set logic

**Q4. Order priority checking**
> For orders placed in Q3 1993, how many had at least one line item received after its commit date, broken down by order priority?

- **Expected:** 5 rows: 1-URGENT 999; 2-HIGH 997; 3-MEDIUM 1031; 4-NOT SPECIFIED 989; 5-LOW 1077.
- **Careful:** says the count is of orders, not line items, and that the check is an EXISTS over line items.

**Q13. Customer distribution**
> How are customers distributed by their number of orders, excluding orders with 'special requests' comments?

- **Expected:** 37 rows, ordered by customer count, highest first. The first is (0 orders, 5,000 customers). Next is (9 orders, 659 customers).
- **Careful:** says customers with zero orders are included, and that the exclusion is applied in the join, not after it.

**Q16. Parts/supplier relationship**
> How many suppliers, without customer complaints, can supply parts that are not Brand#45, not MEDIUM POLISHED, and in sizes 49, 14, 23, 45, 19, 3, 36, 9, by brand, type, and size?

- **Expected:** 2,762 rows. The first is Brand#14, SMALL ANODIZED NICKEL, size 45, with 12 suppliers.
- **Careful:** says the count is distinct suppliers, and how it excluded complaint suppliers (a NOT IN over the complaints comment, not a join that duplicates rows).

**Q21. Suppliers who kept orders waiting**
> Which 100 SAUDI ARABIA suppliers were the only late supplier on a multi-supplier order that could not be fulfilled?

- **Expected:** 47 rows (the limit of 100 doesn't bind). The top is Supplier#000000445 with 16 waits.
- **Careful:** says what "late" means (receipt after commit), and how it tested "only late supplier" (EXISTS and NOT EXISTS over the other line items).

### E. Conditional logic

**Q8. National market share**
> For ECONOMY ANODIZED STEEL parts sold to customers in the AMERICA region in 1995 and 1996, what share of that revenue came from suppliers in BRAZIL, by year?

- **Expected:** 2 rows. 1995: 0.0286 (2.86%). 1996: 0.0183 (1.83%).
- **Careful:** says the share is BRAZIL revenue divided by all AMERICA revenue for that part type in that year, and that it's a fraction, not a percentage.

**Q19. Discounted revenue**
> What was the revenue from air-shipped, in-person-delivered line items matching any of three brand, container, quantity, and size profiles?

- **Expected:** one value, **168,597.29**.
- **Careful:** the hardest one to phrase, so this matters most. Says how it read each of the three profiles (brand, container set, quantity range, size range) and that the three are OR'd. Runs that state their reading and get the number are CORRECT and CAREFUL.

---

## 4. Running the batch

Save this block as `evals.psv` (id | extra flags | prompt). Every row uses the same data file:

```
Q1|--db tpch.duckdb|For line items shipped on or before 1998-09-02, summarize quantity, base price, discounted price, charge, average quantity/price/discount, and line count by return flag and line status.
Q2|--db tpch.duckdb|For brass parts of size 15 in the EUROPE region, which suppliers offer the lowest supply cost? Show the top 100 by supplier account balance, with the part and supplier details.
Q3|--db tpch.duckdb|Which 10 unshipped orders in the BUILDING market segment, placed before 1995-03-15 with items shipping after that date, have the highest potential revenue?
Q4|--db tpch.duckdb|For orders placed in Q3 1993, how many had at least one line item received after its commit date, broken down by order priority?
Q5|--db tpch.duckdb|What was the 1994 revenue from line items where customer and supplier are in the same nation, for each nation in the ASIA region?
Q6|--db tpch.duckdb|How much revenue would have been gained in 1994 by eliminating discounts of 5-7% on line items with quantity under 24?
Q7|--db tpch.duckdb|What was the revenue in 1995 and 1996 from goods shipped between FRANCE and GERMANY (in either direction), by supplier nation, customer nation, and year?
Q8|--db tpch.duckdb|For ECONOMY ANODIZED STEEL parts sold to customers in the AMERICA region in 1995 and 1996, what share of that revenue came from suppliers in BRAZIL, by year?
Q9|--db tpch.duckdb|What was the profit on parts with 'green' in their name, by supplier nation and order year?
Q10|--db tpch.duckdb|Which 20 customers lost the most revenue to returned items on orders placed in Q4 1993?
Q11|--db tpch.duckdb|Which parts in GERMANY make up the most valuable stock (supply cost x available quantity), counting only those above a small fraction of the nation's total?
Q12|--db tpch.duckdb|For MAIL and SHIP line items received in 1994 that were shipped before their commit date but received after it, how many were on high-priority orders (1-URGENT or 2-HIGH) versus other orders, by ship mode?
Q13|--db tpch.duckdb|How are customers distributed by their number of orders, excluding orders with 'special requests' comments?
Q14|--db tpch.duckdb|What share of September 1995 revenue came from promotional parts?
Q15|--db tpch.duckdb|Which supplier generated the most revenue from shipments in Q1 1996?
Q16|--db tpch.duckdb|How many suppliers, without customer complaints, can supply parts that are not Brand#45, not MEDIUM POLISHED, and in sizes 49, 14, 23, 45, 19, 3, 36, 9, by brand, type, and size?
Q17|--db tpch.duckdb|What would the average yearly revenue loss be if Brand#23 MED BOX parts were no longer sold in orders under 20% of their average order quantity?
Q18|--db tpch.duckdb|Which orders have line items totaling more than 300 units? For up to 100 of them, show the customer, order key, order date, total price, and total quantity, largest total price first.
Q19|--db tpch.duckdb|What was the revenue from air-shipped, in-person-delivered line items matching any of three brand, container, quantity, and size profiles?
Q20|--db tpch.duckdb|Which CANADA suppliers have an excess of forest-named parts, with stock above half of what was shipped in 1994?
Q21|--db tpch.duckdb|Which 100 SAUDI ARABIA suppliers were the only late supplier on a multi-supplier order that could not be fulfilled?
Q22|--db tpch.duckdb|Among customers whose phone number starts with country code 13, 31, 23, 29, 30, 18, or 17 and who have never placed an order, how many have an account balance above the average positive balance of customers in those codes, and what is their total balance, by country code?
```

Run them all (each answer goes to `eval_out/<id>.txt`, each log to `eval_out/<id>.log`). `eval_out/` isn't git-ignored, so delete it when you're done or add it to `.gitignore`:

```bash
mkdir -p eval_out
while IFS='|' read -r id flags prompt; do
  echo "== $id"
  uv run agent_harness.py --quiet $flags --prompt "$prompt" < /dev/null > "eval_out/$id.txt" 2> "eval_out/$id.log"
done < evals.psv
```

To compare settings, run the batch again with a flag added (for example `--effort high`); each run's settings are in `agent_runs.config`. Check the cost of a first pass in `agent_runs` before you repeat the full set.

Load the eval list into the traces database once, so runs can be matched to questions. Run it from the traces shell (section 5). The traces server reads the file, so use an absolute path if the server wasn't started from this repo:

```sql
CREATE OR REPLACE TABLE evals AS
SELECT * FROM read_csv('evals.psv', delim = '|', header = false, quote = '',
                       columns = {'id': 'VARCHAR', 'flags': 'VARCHAR', 'prompt': 'VARCHAR'});
```

---

## 5. Trace queries

Run these in the traces shell: `uv run agent_harness.py --traces-shell` (or `duckdb -init agent_traces.duckdb.quack.sql`), which connects to the background traces server, so it works while runs are writing. Replace `<RUN ID>` with a run ID from `agent_runs`.

**Latest run scorecard**
```sql
SELECT r.run_id, r.status, r.total_iterations, r.api_calls, r.duration_ms / 1000 AS seconds,
       round(r.estimated_cost_usd, 4) AS cost_usd, r.config.effort::VARCHAR AS effort,
       (SELECT count(*) FROM tool_calls t WHERE t.run_id = r.run_id) AS tool_calls,
       (SELECT count(*) FROM tool_calls t WHERE t.run_id = r.run_id AND NOT t.success) AS tool_errors,
       (SELECT count(*) FROM tool_calls t WHERE t.run_id = r.run_id AND t.truncated) AS truncated_results
FROM agent_runs r ORDER BY r.started_at DESC LIMIT 1;
```

**Eval scorecard** (latest run of each question; needs the `evals` table above)
```sql
WITH latest AS (
    SELECT e.id, r.*
    FROM evals e JOIN agent_runs r ON r.prompt = e.prompt
    QUALIFY row_number() OVER (PARTITION BY e.id ORDER BY r.started_at DESC) = 1
)
SELECT l.id, l.status, l.total_iterations, round(l.estimated_cost_usd, 4) AS cost_usd,
       count(t.id) AS tool_calls,
       count(t.id) FILTER (WHERE NOT t.success) AS tool_errors,
       count(t.id) FILTER (WHERE t.truncated) AS truncated,
       left(l.final_response, 120) AS answer_start
FROM latest l LEFT JOIN tool_calls t ON t.run_id = l.run_id
GROUP BY ALL ORDER BY l.id;
```

**Data check** (every eval run should be on the scale-0.1 file, not a different one)
```sql
SELECT r.data_source::VARCHAR AS data_source, r.source_db, count(*) AS runs
FROM agent_runs r JOIN evals e ON r.prompt = e.prompt
GROUP BY ALL;
```

**Compare settings** (for example effort levels across the same question set)
```sql
SELECT r.config.effort::VARCHAR AS effort, r.model, count(*) AS runs,
       avg((r.status = 'completed')::INT) AS completed_rate,
       avg(r.total_iterations) AS avg_iterations,
       avg(r.estimated_cost_usd) AS avg_cost_usd,
       avg(r.duration_ms) / 1000 AS avg_seconds
FROM agent_runs r JOIN evals e ON r.prompt = e.prompt
GROUP BY ALL ORDER BY effort;
```

**Runs that didn't complete**
```sql
SELECT started_at, status, stop_reason, error, left(prompt, 80) AS prompt
FROM agent_runs WHERE status <> 'completed' ORDER BY started_at DESC;
```

**Tool errors by class**
```sql
SELECT error_type, count(*) AS errors, any_value(left(error, 120)) AS example
FROM tool_calls WHERE NOT success GROUP BY ALL ORDER BY errors DESC;
```

**Which tables it chose.** For a wrong answer, compare the tables the run used with the *Tables* column in section 3:
```sql
SELECT c.run_id, left(r.prompt, 60) AS prompt, r.answer_tables, c.table_name AS dropped_table,
       c.described, c.sampled, c.queries, c.failed_queries
FROM table_choices c JOIN agent_runs r USING (run_id)
WHERE c.dropped ORDER BY r.started_at DESC, c.first_used_iteration;
```

**Safety check: blocked writes and file access attempts**
```sql
-- Writes, DDL, PRAGMA, SET that the read-only check stopped
SELECT run_id, iteration, left(input_sql, 100) AS sql
FROM tool_calls WHERE error_type = 'read_only_violation';

-- Queries that touched files or URLs (the known gap): review every hit
SELECT run_id, iteration, success, left(input_sql, 120) AS sql
FROM tool_calls
WHERE regexp_matches(lower(input_sql), 'read_(csv|parquet|json|text|blob)|glob\(|https?://|/etc/|~/');
```

**Caching and context per call** (one run)
```sql
SELECT iteration, latency_ms, input_tokens, cache_read_input_tokens, cache_creation_input_tokens,
       output_tokens, round(estimated_cost_usd, 5) AS cost_usd,
       round(cache_read_input_tokens / nullif(input_tokens + cache_read_input_tokens + cache_creation_input_tokens, 0), 3) AS cached_share
FROM api_calls WHERE run_id = '<RUN ID>' ORDER BY id;
```

**Slowest and largest tool calls**
```sql
SELECT duration_ms, result_chars, truncated, left(input_sql, 100) AS sql
FROM tool_calls ORDER BY duration_ms DESC LIMIT 10;
```

**Replay a run step by step**: reasoning, queries, and the answer
```sql
SELECT iteration, action, tool_name,
       left(coalesce(thinking, sql_query, tool_input::JSON::VARCHAR, claude_response), 200) AS detail
FROM agent_traces WHERE run_id = '<RUN ID>' ORDER BY id;
```

**Re-running a query the agent wrote**: the `replay_sql` view (see the README) gives a script you can paste into `duckdb` against `tpch.duckdb`. Print it in list mode:
```sql
.mode list
.headers off
SELECT script FROM replay_sql WHERE run_id = '<RUN ID>' ORDER BY id;
```

**Context clearing events** (long runs only)
```sql
SELECT run_id, iteration, details.cleared_tool_uses, details.cleared_input_tokens
FROM agent_traces WHERE action = 'context_edit';
```

---

## Tips

- **Start with a baseline.** Run all 22 once at the default settings and grade them before changing anything.
- **Change one thing at a time** (effort, a prompt line, a limit), rerun, and compare with *Compare settings* and *Eval scorecard*.
- **Read the thinking** for failed questions (*Replay a run*). It usually shows where the analysis went wrong.
- **Expect some variation between runs.** Run the hard ones (Q2, Q8, Q11, Q13, Q19, Q21) two or three times before concluding a change helped.
- **Watch the scale.** Scale-0.01 numbers (the DH-1 built-in set) don't match these. If a number is off, check the scale first.
