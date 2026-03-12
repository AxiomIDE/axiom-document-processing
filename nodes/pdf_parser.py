from gen.messages_pb2 import PDFInput, DocumentText
from gen.axiom_logger import AxiomLogger, AxiomSecrets


def pdf_parser(log: AxiomLogger, secrets: AxiomSecrets, input: PDFInput) -> DocumentText:
    """Extracts all plain text from a PDF document provided as raw bytes.

    Uses pypdf to read the PDF and concatenates the extracted text from every
    page, separated by newlines. Returns the full text alongside the original
    filename and page count.
    """
    import io
    import pypdf

    log.info("pdf_parser: parsing PDF", filename=input.filename, bytes=len(input.pdf_bytes))

    reader = pypdf.PdfReader(io.BytesIO(input.pdf_bytes))
    page_count = len(reader.pages)

    pages = []
    for i, page in enumerate(reader.pages):
        text = page.extract_text() or ""
        pages.append(text)
        log.debug("pdf_parser: extracted page", page=i + 1, chars=len(text))

    full_text = "\n\n".join(pages).strip()
    log.info("pdf_parser: done", pages=page_count, total_chars=len(full_text))
    return DocumentText(text=full_text, filename=input.filename, page_count=page_count)
