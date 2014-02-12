
all: paper.pdf

paper.pdf: paper.dvi
	dvipdf paper.dvi

paper.dvi:
	latex paper.tex
	bibtex paper
	latex paper.tex
	latex paper.tex

clean:
	rm -f paper.pdf paper.dvi paper.aux paper.bbl paper.blg paper.log

