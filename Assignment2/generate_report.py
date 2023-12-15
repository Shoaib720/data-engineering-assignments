from fpdf import FPDF

WIDTH = 210
HEIGHT = 297

pdf = FPDF()
pdf.add_page()
pdf.set_font('Arial', 'B', 16)
pdf.cell(w=0.9*WIDTH, h=10, txt='DvD Rental Business Summary',align='C',border=0, ln=1)
pdf.image(name='plots/Top 5 Most Revenue Generating Movies.png',w=0.9*WIDTH)
pdf.image(name='plots/Top 10 Most Rented Movies.png',w=0.9*WIDTH)
# pdf.cell(0.9*WIDTH, 10, 'Title', 1, 1, 'C')
pdf.output('reports/sample-report.pdf', 'F')