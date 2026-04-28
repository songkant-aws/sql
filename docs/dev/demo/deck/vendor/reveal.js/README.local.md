# reveal.js vendored assets

This directory is **not checked into the repo** (see `.gitignore`
at `docs/dev/demo/deck/.gitignore`). It holds reveal.js 5.1.0 so
`slides.html` can load offline, which makes `make slides-pdf` work
reliably — CDN loads time out in Chrome headless cold-cache runs.

## First-time setup

From `docs/dev/demo/deck`:

```bash
mkdir -p vendor/reveal.js
cd vendor/reveal.js

curl -L -o reveal.zip \
  https://github.com/hakimel/reveal.js/archive/refs/tags/5.1.0.zip
unzip -q reveal.zip
mv reveal.js-5.1.0/* .
rm -rf reveal.js-5.1.0 reveal.zip

cd ../..
```

## Sanity check

```bash
ls vendor/reveal.js/dist/reveal.js                 # exists
ls vendor/reveal.js/dist/theme/white.css           # exists
ls vendor/reveal.js/plugin/highlight/highlight.js  # exists
ls vendor/reveal.js/plugin/notes/notes.js          # exists
```

All four must be present. If any is missing, re-run the setup
block above.

## Why vendor instead of CDN

- CDN path (`cdn.jsdelivr.net/npm/reveal.js@5.1.0/...`) works fine
  when you `make slides` and drive the deck interactively in a
  warm browser — the browser has the scripts cached from a prior
  visit.
- `make slides-pdf` starts Chrome headless cold each time. The
  `--virtual-time-budget` flag controls how long Chrome will wait
  for async resources before snapshotting the DOM to PDF, and
  even 30 seconds wasn't enough on some cold-cache runs, yielding
  a ~1 KB empty PDF.
- Vendored assets resolve instantly from disk, so `make slides-pdf`
  completes in a few seconds regardless of network.

## Why not check in

- ~4 MB of CSS/JS/themes isn't demo content — it's toolchain.
- If reveal.js needs a bump later (5.1.1, 6.x), refreshing a
  local copy is one command, vs. changing a commit across many
  files.
