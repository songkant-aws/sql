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

## Status of the numbers

The markdown has placeholder `_TBD_` values in the benchmark table for
Path B and Path C latencies. These get filled in after running
`query-b.sh` and `query-c.sh` on the federation environment (see
`../path-bc/README.md`).

Numbers already populated (measured):

- Path A ingest time: **3h 36m**
- Path A OS index size: **19.3 GB**
- Path A slow-join latency: **30 s**
- Path A simple-join rows returned: **2** (silent truncation, expected 50)
- Path B/C ClickHouse ingest time: **~7 min** (4m 20s ingest + 2m 12s optimize)
- Path B/C ClickHouse storage: **7 GB** on-disk

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
| Three paths | 2 | 30 s |
| Benchmark table | 2 | 60 s |
| Architecture diagram | 3 | 45 s |
| Business implications | 3 | 30 s |
| Ask | 4 | 15 s |

Total: ~4 min 30 s — aim for the lower end, leave 30 s for first question.

## Updating numbers after Path B/C benchmark

When you run `query-b.sh` / `query-c.sh` (on Machine A in the federation
environment), copy the warm-median latencies into the markdown table,
then rebuild:

```bash
# Edit ppl-federation-demo.md — find `_TBD_` and fill in.
make pdf
```

The four `_TBD_` cells to fill:

- Path B slow join latency
- Path C slow join latency
- Path B simple join latency
- Path C simple join latency
- Path B simple join rows returned

Also revisit the closing line ("30 s → sub-second") to match the actual
measured Path C latency.
