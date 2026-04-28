# Build the PPL federation demo deck as a PDF.
#
# Targets:
#   make                 → default, produces ppl-federation-demo.pdf via pandoc+xelatex
#   make pdf             → same as above
#   make html            → standalone HTML (works with any browser, no LaTeX needed)
#   make pdf-via-html    → pandoc HTML → wkhtmltopdf PDF (no LaTeX needed)
#   make preview         → open the PDF in the default viewer
#   make clean           → remove generated artifacts
#
# Install prerequisites:
#
#   macOS (Homebrew):
#     brew install pandoc
#     brew install --cask mactex-no-gui     # xelatex (~4 GB); or:
#     brew install --cask wkhtmltopdf       # if using pdf-via-html
#
#   Amazon Linux 2023:
#     sudo dnf install -y pandoc texlive-xetex texlive-collection-latex
#
#   Debian/Ubuntu:
#     sudo apt install pandoc texlive-xetex texlive-latex-extra

SRC          := ppl-federation-demo.md
PDF          := ppl-federation-demo.pdf
HTML         := ppl-federation-demo.html
PDF_VIA_HTML := ppl-federation-demo-html.pdf

# Shared pandoc options. --pdf-engine picks the LaTeX renderer (xelatex is
# nicer for UTF-8 / CJK than pdflatex). -V geometry keeps margins tight so
# the deck fits on a single printable page range.
PANDOC_OPTS := \
  --from markdown+yaml_metadata_block \
  -V geometry:margin=0.9in \
  -V fontsize=11pt \
  -V colorlinks=true \
  -V linkcolor=blue \
  --highlight-style=tango \
  --toc-depth=2

.PHONY: all pdf html pdf-via-html preview clean help

all: pdf

pdf: $(PDF)

$(PDF): $(SRC)
	@which pandoc   >/dev/null || { echo "ERROR: pandoc not installed. See Makefile header."; exit 1; }
	@which xelatex  >/dev/null || { echo "ERROR: xelatex not installed (try 'make pdf-via-html' instead)."; exit 1; }
	pandoc $(PANDOC_OPTS) --pdf-engine=xelatex -o $@ $<
	@echo ""
	@echo "Built: $@"

html: $(HTML)

$(HTML): $(SRC)
	@which pandoc >/dev/null || { echo "ERROR: pandoc not installed."; exit 1; }
	pandoc $(PANDOC_OPTS) --standalone --self-contained -o $@ $<
	@echo ""
	@echo "Built: $@"
	@echo "Open in a browser; use Cmd/Ctrl+P → Save as PDF for a quick PDF."

pdf-via-html: $(PDF_VIA_HTML)

$(PDF_VIA_HTML): $(HTML)
	@which wkhtmltopdf >/dev/null || { echo "ERROR: wkhtmltopdf not installed. See Makefile header."; exit 1; }
	wkhtmltopdf \
	  --margin-top 15mm --margin-bottom 15mm \
	  --margin-left 20mm --margin-right 20mm \
	  --enable-local-file-access \
	  $< $@
	@echo ""
	@echo "Built: $@"

preview: pdf
	@if [ "$$(uname)" = "Darwin" ]; then open $(PDF); \
	 elif command -v xdg-open >/dev/null; then xdg-open $(PDF); \
	 else echo "open $(PDF) manually"; fi

clean:
	rm -f $(PDF) $(HTML) $(PDF_VIA_HTML)

help:
	@awk 'BEGIN{p=0} /^# /{if(p)exit; print} /^$$/{p=1}' $(lastword $(MAKEFILE_LIST))
