# PPL Federation - 4-minute demo transcript

Target: about 4:00 spoken, with a small buffer for slide changes.
Pacing: keep the results slide slow; move quickly through RAG and
architecture unless the room asks for details.

Each section marks **[on slide]** with the title in `slides.html`,
followed by the words to say. Stage directions are in *italic*.

---

## [on slide 1 - Title] - 10 s

*Let the title sit for a beat.*

"PPL Federation. The idea is simple: keep OpenSearch as the entry
point, and use the right engine for each workload."

---

## [on slide 2 - The opportunity] - 25 s

"OpenSearch is already where customers come for search, relevance,
Dashboards, and observability. That part is working.

The gap appears when the same workflow needs fact-table analytics:
reviews, clickstream, orders, long-term spans. Today customers either
duplicate that data into OpenSearch, or stitch the answer together in
application code.

Both choices create an ETL tax that grows with the data."

---

## [on slide 3 - What customers work around today] - 25 s

"Here is that tax in a concrete workload: Amazon Reviews 2023, with
3.7 million products and 67 million reviews.

Keeping everything in OpenSearch means almost three hours per review
duplication cycle, 19 gigabytes instead of about 7, and about 30
seconds for a dashboard-style group-by.

Those are the visible costs. One architectural limit shows up only at
scale."

---

## [on slide 4 - Single-engine paths hit a wall] - 25 s

*Slow down slightly.*

"PPL's subsearch cap is correct by design. It prevents an unbounded
join from exhausting memory.

But at fact-table scale, that safety boundary becomes a ceiling. In
this workload, the single-engine path returns 2 rows where 14 are
actually correct.

So the fix is not simply a bigger cap. The fix is to stop asking one
engine to do two very different jobs."

---

## [on slide 5 - Three execution paths] - 25 s

"That gives us three paths, with the same customer-facing PPL query.

Path A is today's default: products and reviews both in OpenSearch.

Path B keeps products in OpenSearch and reviews in ClickHouse. PPL
pushes the analytical subtree to ClickHouse as one JDBC query.

Path C adds the planner optimization: take the bounded search result
from OpenSearch and bind those keys into a ClickHouse `WHERE IN`
filter.

The customer does not learn a new language. The planner chooses the
backend."

---

## [on slide 6 - Results] - 45 s

*Pause on the table. This is the core slide.*

"Same dataset, same query shape, measured end to end on the
67-million-row fact table.

The headline is not only speed. It is that each system does the part it
is good at.

Ingest drops from 2 hours 50 minutes to 4 minutes 20 seconds. Storage
drops from 19.3 gigabytes to 7.24.

Latency drops from 30 seconds to about 195 milliseconds, around 154x
end to end.

ClickHouse shows why: with IN-list pushdown, it scans 360 thousand
rows instead of 67 million, and the engine work is 7 to 8 milliseconds.

Most importantly, Path A loses most of the grouped result. Paths B and
C return the correct 14."

---

## [on slide 7 - Three layers stack cleanly] - 25 s

"The speedup comes from three layers.

First, engine selection: columnar storage and hash aggregation replace
paginated inverted-index aggregation.

Second, IN-list pushdown: the planner ships the 50 bounded search keys
to ClickHouse, so the primary-key index skips almost all of the fact
table.

Third, correctness: the fact-side work is no longer boxed in by the
single-engine subsearch cap.

And the PPL stays unchanged."

---

## [on slide 8 - RAG] - 20 s

"The same pattern matters beyond retail.

For RAG, swap BM25 for k-NN. OpenSearch retrieves the top documents;
the external fact store enriches them with interactions or business
metrics.

That gives one query entry point: relevance in OpenSearch, facts in
the system built for facts."

---

## [on slide 9 - Architecture + PPL code] - 20 s

*Point at the highlighted query lines.*

"Mechanically, the customer-visible delta is tiny.

`match(title, ...)` is BM25 search in OpenSearch.

`source=ch.fed.reviews` is the federation part: one prefix naming the
external source.

Calcite builds the plan, pushes the JDBC subtree down, and ClickHouse
scans the relevant fact rows instead of the whole table."

---

## [on slide 10 - Summary & next steps] - 20 s

*Close calmly.*

"So the message is: OpenSearch's strengths stay strengths. PPL,
Dashboards, BM25, k-NN, auth and observability integration all remain
the entry point.

Federation lets fact tables live where they belong.

The branch is code complete and benchmarked. The next step is a retail
or observability design partner, with ClickHouse as the first reference
datasource.

Happy to take questions."

*Stop talking. Do not fill the silence.*

---

## [if asked a technical question - appendix] - variable

"Here is the physical plan for Path C.

At the bottom is the JDBC table scan. Above it is a filter with
`ARRAY_IN($0, ?0)`. That parameter is the runtime key array collected
from the OpenSearch side.

The rest of the JDBC subtree is still pushed as one SQL query, so the
optimization does not introduce a second customer-visible query."

---

## Pacing check

| Section | Length | Running total |
|---|---:|---:|
| 1. Title | 10 s | 0:10 |
| 2. Opportunity | 25 s | 0:35 |
| 3. Today's tax | 25 s | 1:00 |
| 4. Single-engine wall | 25 s | 1:25 |
| 5. Three paths | 25 s | 1:50 |
| 6. Results | 45 s | 2:35 |
| 7. Speedup layers | 25 s | 3:00 |
| 8. RAG | 20 s | 3:20 |
| 9. Architecture | 20 s | 3:40 |
| 10. Close | 20 s | 4:00 |

## Delivery notes

- **Main story:** search entry point stays in OS; fact-scale analytics
  moves to the engine built for it.
- **Slide 4:** keep the tone calm. The cap is a safety boundary, not a
  bug. The point is that workload routing is the cleaner answer.
- **Slide 6:** say only the headline rows: ingest, storage, latency,
  rows scanned, correctness. Let the rest of the table support you.
- **Slide 8 and 9:** these are proof that the idea generalizes and is
  simple to use. Do not over-explain them in the main 4-minute run.
- **Appendix:** open only if asked about the planner mechanics.

## Optional cuts for a strict room

- Skip slide 8's second sentence and say only: "The same federation
  pattern also covers RAG: OpenSearch retrieves, the fact store
  enriches." Saves about 10 seconds.
- On slide 6, if time is tight, skip the storage row and go directly
  from ingest to latency. Saves about 8 seconds.
