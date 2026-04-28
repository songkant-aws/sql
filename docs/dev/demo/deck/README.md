# PPL federation demo deck

4-minute leadership pitch for the PPL federation feature on
`feat/ppl-federation`.

## Files

| File | Purpose |
|---|---|
| `ppl-federation-demo.md` | Source. Markdown with pandoc YAML frontmatter. |
| `Makefile` | Build PDF or HTML via pandoc. |
| `README.md` | This file. |
| `architecture.txt` | Standalone ASCII diagram (also embedded inline in the md). |

## Numbers (all measured end-to-end on 2026-04-28)

Benchmark environment:

- **Dataset**: Amazon Reviews 2023 (Home_and_Kitchen subset)
  — 3.7 M products, 67 M reviews
- **Path A** (all-in-OS): `r6i.2xlarge`, OS 3.6.0, 24 GB heap, products + reviews both in OS
- **Path B** (federation): two-node VPC-internal deployment
  — Machine A: `r6i.xlarge` OS 3.6.0 (products only)
  — Machine B: `r6i.2xlarge` CH 24.8 (reviews only)
  — CH connected via `jdbc:ch://<vpc-ip>:8123/default?compress=0`,
    SG-restricted to Machine A

### Core numbers

| Metric | Path A | Path B | Note |
|---|---:|---:|---|
| Products ingest (~3.7 M) | 27 min | 28 min | Both index products into OS |
| Reviews ingest (67 M) | **2h 50m** | **4m 20s** | ~40× |
| Reviews storage | 19.3 GB (OS) | 7.24 GB (CH) | 2.7× smaller |
| Slow-join end-to-end (warm median) | **30 s** | **~600 ms** | ~50× |
| Slow-join CH engine only | — | **465 ms** | pure aggregation |
| Simple-join end-to-end (warm) | 300 ms | ~2.3 s | Path A wrong |
| Simple-join rows returned | 2 (96 % loss) | 1197 | correctness |

### Path A "slow-join" query (28 s baseline):

```ppl
source=reviews | stats count() as n, avg(rating) as avg_r by parent_asin | sort -n | head 20
```

Path A does composite-aggregation pagination through ~3.78 M distinct
`parent_asin` groups. `read_rows` on CH for the same aggregation:
67 M in ~465 ms.

### Known follow-up

`SideInputInListRule` (IN-list sideways pushdown) is implemented but
has a runtime binder bug: the array parameter resolves to `NULL` at
execution, causing CH to receive `WHERE parent_asin IN (NULL)` and
return 0 rows. Full repro + suspected root cause captured in
`docs/dev/bug-in-list-pushdown-null-binding.md`. Not needed for the
core value demonstrated above.

## Build

```bash
# PDF (requires pandoc + xelatex)
make

# HTML (no LaTeX needed, any browser)
make html

# PDF via HTML + wkhtmltopdf (no LaTeX)
make pdf-via-html

# Open the PDF
make preview
```

See the `Makefile` header for install instructions on macOS, Amazon Linux,
Debian/Ubuntu.

## Speaking notes (~4 minutes)

| Section | Page | Spoken length |
|---|---|---|
| Problem | 1 | 30 s |
| Three failure modes (Path A) | 1 | 60 s |
| Two paths | 2 | 30 s |
| Benchmark table | 2 | 60 s |
| Architecture diagram | 3 | 45 s |
| Business implications | 3 | 30 s |
| Ask | 4 | 15 s |

Total: ~4 min 30 s — aim for the lower end, leave 30 s for first question.
