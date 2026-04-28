# Build the PPL federation demo deck.
#
# Two deliverables live here:
#
#   1. slides.html — reveal.js slide deck, intended for demo presentation.
#      Open in a browser and drive with arrow keys. This is the primary
#      output for the 4-minute leadership pitch.
#
#   2. ppl-federation-demo.md — the long-form markdown brief with full
#      benchmark table. Kept for written circulation / design-review
#      pre-reads.
#
# Targets (run with `make <target>`):
#
#   make slides         → open slides.html locally (default)
#   make slides-pdf     → export slides.html to a print-ready PDF via
#                          headless Chrome / Chromium. No LaTeX needed.
#   make brief          → export the markdown brief to PDF via pandoc+xelatex
#   make brief-html     → standalone HTML of the brief
#   make pdf-via-html   → brief HTML → PDF via wkhtmltopdf (no LaTeX)
#   make clean          → remove generated artifacts
#
# Install prerequisites:
#
#   macOS (Homebrew):
#     brew install pandoc              # for the brief
#     brew install --cask google-chrome  # or chromium, or use system Chrome
#     brew install --cask mactex-no-gui  # only if building brief as LaTeX PDF
#
#   Amazon Linux 2023:
#     sudo dnf install -y pandoc chromium texlive-xetex
#
#   Debian/Ubuntu:
#     sudo apt install pandoc chromium texlive-xetex

SLIDES_HTML  := slides.html
SLIDES_PDF   := slides.pdf

BRIEF_SRC    := ppl-federation-demo.md
BRIEF_PDF    := ppl-federation-demo.pdf
BRIEF_HTML   := ppl-federation-demo.html
PDF_VIA_HTML := ppl-federation-demo-html.pdf

# reveal.js exposes a PDF-export layout when the URL carries ?print-pdf.
# Chrome's headless --print-to-pdf renders that layout correctly when
# given enough width/height to match the reveal slide dimensions (1280x720).
#
# Find a Chrome/Chromium binary at run time; operator may have any of them.
CHROME := $(shell \
  for c in google-chrome-stable google-chrome chromium chromium-browser; do \
    if command -v $$c >/dev/null 2>&1; then echo $$c; break; fi; \
  done; \
  if [ -x "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" ]; then \
    echo "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"; \
  fi \
)

PANDOC_OPTS := \
  --from markdown+yaml_metadata_block \
  -V geometry:margin=0.9in \
  -V fontsize=11pt \
  -V colorlinks=true \
  -V linkcolor=blue \
  --highlight-style=tango

.PHONY: all slides slides-pdf brief brief-html pdf-via-html preview-slides preview-brief clean help

all: slides-pdf

# ---- Slides (primary) -----------------------------------------------------

slides: $(SLIDES_HTML)
	@if [ "$$(uname)" = "Darwin" ]; then open $(SLIDES_HTML); \
	 elif command -v xdg-open >/dev/null; then xdg-open $(SLIDES_HTML); \
	 else echo "open $(SLIDES_HTML) in a browser"; fi

slides-pdf: $(SLIDES_PDF)

$(SLIDES_PDF): $(SLIDES_HTML)
	@if [ -z "$(CHROME)" ]; then \
	  echo "ERROR: Chrome/Chromium not found. Install Google Chrome or chromium."; \
	  echo "       macOS:  brew install --cask google-chrome"; \
	  echo "       AL2023: sudo dnf install -y chromium"; \
	  exit 1; \
	fi
	@echo "Using browser: $(CHROME)"
	"$(CHROME)" \
	  --headless=new \
	  --no-sandbox \
	  --disable-gpu \
	  --no-pdf-header-footer \
	  --virtual-time-budget=30000 \
	  --run-all-compositor-stages-before-draw \
	  --hide-scrollbars \
	  --print-to-pdf-no-header \
	  --print-to-pdf="$(abspath $(SLIDES_PDF))" \
	  "file://$(abspath $(SLIDES_HTML))?print-pdf"
	@# Sanity: an empty reveal.js PDF is usually <10KB (no slides rendered).
	@# A real 11-slide deck is ~500KB–2MB. Warn the operator if the
	@# virtual-time-budget wasn't long enough for the CDN to load.
	@size=$$(stat -f%z "$(SLIDES_PDF)" 2>/dev/null || stat -c%s "$(SLIDES_PDF)"); \
	 if [ "$$size" -lt 50000 ]; then \
	   echo ""; \
	   echo "WARNING: $(SLIDES_PDF) is only $$size bytes — slides didn't render."; \
	   echo "  Likely cause: CDN load timed out. Bump --virtual-time-budget=30000"; \
	   echo "  in the Makefile higher (try 60000), or pre-fetch reveal.js locally."; \
	   echo ""; \
	 fi
	@echo ""
	@echo "Built: $(SLIDES_PDF)"
	@echo "Tip:   open the PDF and verify each slide renders on its own page."
	@echo "       If slides are cut off, increase viewport via printSize= in slides.html."

preview-slides: $(SLIDES_PDF)
	@if [ "$$(uname)" = "Darwin" ]; then open $(SLIDES_PDF); \
	 elif command -v xdg-open >/dev/null; then xdg-open $(SLIDES_PDF); \
	 else echo "open $(SLIDES_PDF) manually"; fi

# ---- Long-form brief (optional) -------------------------------------------

brief: $(BRIEF_PDF)

$(BRIEF_PDF): $(BRIEF_SRC)
	@which pandoc  >/dev/null || { echo "ERROR: pandoc not installed."; exit 1; }
	@which xelatex >/dev/null || { echo "ERROR: xelatex not installed (try 'make pdf-via-html')."; exit 1; }
	pandoc $(PANDOC_OPTS) --pdf-engine=xelatex -o $@ $<
	@echo ""
	@echo "Built: $@"

brief-html: $(BRIEF_HTML)

$(BRIEF_HTML): $(BRIEF_SRC)
	@which pandoc >/dev/null || { echo "ERROR: pandoc not installed."; exit 1; }
	pandoc $(PANDOC_OPTS) --standalone --embed-resources -o $@ $<
	@echo ""
	@echo "Built: $@"

pdf-via-html: $(PDF_VIA_HTML)

$(PDF_VIA_HTML): $(BRIEF_HTML)
	@which wkhtmltopdf >/dev/null || { echo "ERROR: wkhtmltopdf not installed."; exit 1; }
	wkhtmltopdf \
	  --margin-top 15mm --margin-bottom 15mm \
	  --margin-left 20mm --margin-right 20mm \
	  --enable-local-file-access \
	  $< $@
	@echo ""
	@echo "Built: $@"

preview-brief: $(BRIEF_PDF)
	@if [ "$$(uname)" = "Darwin" ]; then open $(BRIEF_PDF); \
	 elif command -v xdg-open >/dev/null; then xdg-open $(BRIEF_PDF); \
	 else echo "open $(BRIEF_PDF) manually"; fi

# ---- Housekeeping ---------------------------------------------------------

clean:
	rm -f $(SLIDES_PDF) $(BRIEF_PDF) $(BRIEF_HTML) $(PDF_VIA_HTML)

help:
	@awk 'BEGIN{p=0} /^# /{if(p)exit; print} /^$$/{p=1}' $(lastword $(MAKEFILE_LIST))
