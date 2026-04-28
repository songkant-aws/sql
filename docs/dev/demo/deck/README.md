# PPL federation demo deck

4-minute leadership pitch for the PPL federation feature on
`feat/ppl-federation`. Two deliverables:

1. **`slides.html`** — self-contained reveal.js slide deck for the
   4-minute presentation. Open in any browser, drive with arrow keys.
   Speaker notes embedded in each slide (press `S` to open notes view).
   This is the primary artifact.
2. **`ppl-federation-demo.md`** — long-form markdown brief with the
   full benchmark table. Use for pre-read circulation before the
   pitch or as a leave-behind after.

## Files

| File | Purpose |
|---|---|
| `slides.html` | Slide deck (reveal.js) — demo this. |
| `ppl-federation-demo.md` | Long-form brief (markdown, pandoc). |
| `architecture.txt` | Standalone ASCII diagram. |
| `Makefile` | Build PDF exports (both slides and brief). |
| `README.md` | This file. |

## Build the slide PDF

```bash
make slides-pdf       # Chrome headless → slides.pdf
make preview-slides   # Open the PDF

# Or just open the HTML in a browser for live presentation:
make slides
```

Prerequisite: Google Chrome or Chromium. Install:
- macOS: `brew install --cask google-chrome`
- Amazon Linux 2023: `sudo dnf install -y chromium`

## Build the brief PDF (optional)

```bash
make brief            # pandoc + xelatex → ppl-federation-demo.pdf
make brief-html       # pandoc → standalone HTML
make pdf-via-html     # pandoc + wkhtmltopdf (no LaTeX)
```

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

| Metric | Path A | Path B | Path C |
|---|---:|---:|---:|
| Products ingest (~3.7 M) | 27 min | 28 min | 28 min |
| Reviews ingest (67 M) | **2h 50m** | **4m 20s** | 4m 20s |
| Reviews storage | 19.3 GB (OS) | 7.24 GB (CH) | 7.24 GB (CH) |
| End-to-end latency (warm median) | **30 s** | ~600 ms | **~170 ms** |
| ClickHouse engine-only | — | 465 ms | **6–7 ms** |
| Fact rows scanned at CH | — | 67 M | **360 K** (187× less) |
| Fact bytes read at CH | — | 1.94 GB | **8.48 MB** (233× less) |
| End-to-end A→C speedup | — | — | **~176×** |

### Path A "slow-join" baseline

```ppl
source=reviews | stats count() as n, avg(rating) as avg_r by parent_asin | sort -n | head 20
```

Path A does composite-aggregation pagination through ~3.78 M distinct
`parent_asin` groups (378 round-trips × ~75 ms each = ~30 s).

### Path C query (IN-list pushdown active)

```ppl
source=products | where match(title, 'stainless steel cookware')
| sort -_score | head 50
| inner join left=p right=r on p.parent_asin = r.parent_asin
  [ source=ch.fed.reviews | where timestamp > 1640995200000
    | fields parent_asin, rating ]
| stats avg(rating) as avg_r, count() as n by parent_asin
```

Explain shows `JdbcFilter(ARRAY_IN($0, ?0))` in the physical plan;
ClickHouse `system.query_log` records the SQL with the actual 50
parent_asin values inlined into `WHERE parent_asin IN (…)`.

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
