from gen.messages_pb2 import DownloadRequest, PDFInput
from gen.axiom_logger import AxiomLogger, AxiomSecrets


def pdf_downloader(log: AxiomLogger, secrets: AxiomSecrets, input: DownloadRequest) -> PDFInput:
    """Downloads a PDF from a URL and returns the raw bytes ready for parsing.

    Uses urllib from the Python standard library — no extra dependencies needed.
    The filename is derived from the last segment of the URL path when the
    optional filename field is not provided.
    """
    import urllib.request
    from urllib.parse import urlparse

    filename = input.filename if input.filename else urlparse(input.url).path.split("/")[-1] or "document.pdf"

    log.info("pdf_downloader: fetching PDF", url=input.url, filename=filename)
    with urllib.request.urlopen(input.url) as response:
        pdf_bytes = response.read()
    log.info("pdf_downloader: downloaded", filename=filename, bytes=len(pdf_bytes))

    return PDFInput(pdf_bytes=pdf_bytes, filename=filename)
