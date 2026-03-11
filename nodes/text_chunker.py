from gen.messages_pb2 import ChunkRequest, ChunkedDocument
from gen.axiom_logger import AxiomLogger, AxiomSecrets

DEFAULT_CHUNK_SIZE = 1000
DEFAULT_OVERLAP = 200


def text_chunker(log: AxiomLogger, secrets: AxiomSecrets, input: ChunkRequest) -> ChunkedDocument:
    """Splits a text string into overlapping fixed-size chunks suitable for embedding.

    Uses character-level splitting with configurable chunk_size and overlap.
    Defaults to chunk_size=1000 and overlap=200 when the input fields are zero.
    Chunks are trimmed of leading/trailing whitespace and empty chunks are dropped.
    """
    chunk_size = input.chunk_size if input.chunk_size > 0 else DEFAULT_CHUNK_SIZE
    overlap = input.overlap if input.overlap >= 0 else DEFAULT_OVERLAP

    if overlap >= chunk_size:
        log.warn("text_chunker: overlap >= chunk_size, clamping overlap", chunk_size=chunk_size, overlap=overlap)
        overlap = chunk_size // 4

    text = input.text
    step = chunk_size - overlap
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        start += step

    log.info("text_chunker: done", chunk_count=len(chunks), chunk_size=chunk_size, overlap=overlap)
    return ChunkedDocument(chunks=chunks, source_filename=input.source_filename)
