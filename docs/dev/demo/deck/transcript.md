# PPL Federation — 4-minute pitch transcript

Target: 4:00 spoken + 30 s buffer for transitions and questions.
Pacing: ~160 words per minute on the main flow, slower on the two
number-heavy slides.

Each section marks **[on slide]** with the slide title in `slides.html`,
followed by the words to say. Stage directions in *italic*.

---

## [on slide 1 — Title] — 15 s

*Walk up, let the title sit on screen for a beat.*

"PPL Federation. The short version: **same entry point, right engine
for each workload.** Four minutes."

---

## [on slide 2 — The opportunity] — 30 s

"OpenSearch excels at search, relevance, observability. That's why
customers stay on it.

But as their workload expands to **fact-table analytics** —
reviews, clickstream, orders, long-term spans — they need the right
engine for *that* work too.

Today, the only paths are to **duplicate the fact data into OS**, or
to **stitch results together in application code**. Either way, the
customer is paying an **ETL tax that scales with their data**."

---

## [on slide 3 — What customers work around today] — 40 s

"Here's what that tax looks like in a real workload.

**Amazon Reviews 2023** — a public UCSD dataset. 3.7 million products,
67 million reviews. End-to-end measurements, warm-run medians.
Reproduction scripts in the repo.

*Point at table rows.*

Teams wait nearly three hours per duplication cycle. Storage grows
2.7× because inverted index inflates against raw documents. Dashboard
iteration is 30 seconds per cycle, because OS has to walk 3.78 million
groups for one top-K query.

*→ Transition:* **Three visible costs. Manageable. But there's a fourth that teams don't see coming.**"

---

## [on slide 4 — Single-engine paths hit a wall] — 30 s

*Let the numbers land. Slow tempo.*

"PPL's join subsearch bounds output to 50 000 rows. That's
correct-by-design — it's what stops the engine from running out of
memory on an unbounded join.

At fact-table scale — millions of distinct keys — **that protection
becomes a ceiling**. Two rows come back where around fifty would
answer the business question.

The fix isn't a larger cap. It's a different engine for the fact
side."

---

## [on slide 5 — Three paths] — 25 s

"Three paths. **Same PPL query in all three.**

- **Path A:** everything in OS. Today's default.
- **Path B:** products in OS, reviews in ClickHouse. PPL pushes the
  analytical work to CH as a single JDBC query.
- **Path C:** same as B, plus a planner rule that binds the
  search-side keys into a `WHERE IN` filter on the CH query at runtime.

The customer writes PPL the same way either way. The planner picks
the backend."

---

## [on slide 6 — Results] — 60 s

*Pause on the results table. This is the core.*

"**Same dataset, same machines, three execution paths.** Everything
end-to-end on the 67-million-row fact table.

Each path uses each engine where it excels: Path A asks one engine to
do two jobs; B and C let each engine do what it's built for.

*Walk through the rows.*

**Ingest:** two hours fifty down to four minutes twenty — forty times.

**Storage:** nineteen gigabytes in OS down to seven in ClickHouse —
columnar compression at work.

**Latency:** thirty seconds down to one-seventy milliseconds. One
hundred seventy-six times end-to-end. The ClickHouse engine itself
does the aggregation in six milliseconds — it scans three-hundred-sixty
thousand rows instead of sixty-seven million.

**And**: Path A returned two rows where fifty were expected. Paths B
and C return the complete eleven-hundred-ninety-seven.

*→ Transition:* **176× doesn't happen by accident. Here's how it decomposes.**"

---

## [on slide 7 — Three layers stack cleanly] — 45 s

"Three independent speedups stack cleanly on top of each other.

**Engine selection** — A to B. Columnar storage and hash aggregation
replace paginated inverted-index walks.

**IN-list pushdown** — B to C. The planner drains the bounded left
side's fifty keys and ships them as `WHERE IN` to ClickHouse.
MergeTree's primary-key index skips more than ninety-nine percent of
partitions.

**Join semantics** — everywhere below A. The 50-K cap disappears
because the work is no longer constrained by a single-engine safety
default.

The customer-facing PPL is unchanged. And the rule framework is
**JDBC-dialect agnostic** — Iceberg, Snowflake, other targets plug in
next."

---

## [on slide 8 — RAG] — 25 s

"Same machinery, new market.

*Point at the PPL code.*

Swap `match` for `knn`, and the same federation carries the canonical
**retrieval-augmented analytics** pattern. OS does k-NN top-K.
ClickHouse does the fact enrichment.

OpenSearch is the only engine with **k-NN, BM25, and external-fact
federation all GA**. ClickHouse has no mature relevance scoring.
Vector DBs have no business-data federation.

This is OpenSearch's lane."

---

## [on slide 9 — Architecture + PPL code] — 25 s

"The whole customer delta is **one prefix**.

*Point at the yellow highlight.*

`match(title, ...)` — BM25 search. OS's native strength.

*Point at the blue highlight.*

`source=ch.fed.reviews` — the whole federation delta. Customer
declares the source; the planner does the rest.

One PPL query. Two storages. One entry point."

---

## [on slide 10 — Summary & next steps] — 25 s

*Close calmly.*

"**OS's strengths stay strengths.** PPL, BM25, k-NN, Dashboards,
observability integration — all unchanged. The search entry point
also becomes the federation entry point.

Code is complete on the feat branch. Benchmarked end-to-end.

If we take this forward, two natural next steps: **engage a design
partner** — retail or observability — and start with ClickHouse as
the reference first datasource; additional JDBC targets plug in
incrementally.

Happy to take questions."

*Stop talking. Don't fill silence.*

---

## [if asked a technical question — flip to appendix] — variable

"Here's the physical plan the planner builds for Path C.

*Point bottom-up.*

At the bottom, the JDBC table scan.

Above it, a filter containing `ARRAY_IN($0, ?0)` — that's the IN-list
the rule inserted. The `?0` is a runtime array parameter; the binder
fills it with the fifty keys drained from the OS side.

Above that, the rest of the JDBC subtree — sort, project, converter —
all translated into one SQL query and shipped to ClickHouse.

Three regions. Color-coded for whoever's reading this later."

---

## Pacing check

| Section | Length | Running total |
|---|---|---|
| 1. Title | 15 s | 0:15 |
| 2. Opportunity | 30 s | 0:45 |
| 3. Today's tax (with methodology) | 40 s | 1:25 |
| 4. Single-engine wall | 30 s | 1:55 |
| 5. Three paths | 25 s | 2:20 |
| 6. Results (with provenance one-liner) | 60 s | 3:20 |
| 7. Speedup layers | 45 s | 4:05 |
| 8. RAG | 25 s | 4:30 |
| 9. Architecture | 25 s | 4:55 |
| 10. Close | 25 s | 5:20 |

Tight for a hard 4-minute slot. Cut candidates if the room is strict:

- **Drop slide 8 (RAG)** if the audience is retail- or
  observability-only. Saves 25 s, keeps the mainline story intact.
- **Shorten slide 7 to 30 s** by skipping the "three layers" framing
  and jumping directly from "A to B columnar engine" to "B to C
  IN-list pushdown". Saves 15 s.
- **Cut slide 9 (architecture)** if the results table and RAG
  examples already made the mechanism clear. Saves 25 s.

The most compressible 4:00 version is: 1, 2, 3, 4, 5, 6, 7 (trimmed),
10. That's **3:35** — lets you open to questions with a visible
buffer.

## Delivery notes

- **Slide 4 (Silent-style, calm variant):** this is the hook. Don't
  rush it. Let "two of fifty" sit on screen for a beat before the
  "fix isn't a larger cap" line.
- **Slide 6 (Results):** the table is dense. Say the three headline
  numbers (40x, 176x, 6 ms) slowly and clearly; let the rest of the
  table speak visually.
- **Slide 10 (Close):** end on "Happy to take questions" and stop.
  The summary slide already carries the next-steps list visually;
  don't re-read it.
- **Appendix:** only open if asked. Opening it unprompted signals
  "I prepared too much" and erodes the close.
