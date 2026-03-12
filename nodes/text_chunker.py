from typing import Iterator
from gen.messages_pb2 import ChunkRequest, ChunkItem
from gen.axiom_logger import AxiomLogger, AxiomSecrets

DEFAULT_CHUNK_SIZE = 1000
DEFAULT_OVERLAP = 200


def text_chunker(log: AxiomLogger, secrets: AxiomSecrets, inputs: Iterator[ChunkRequest]) -> Iterator[ChunkItem]:
    """Splits a text string into overlapping fixed-size chunks and streams one ChunkItem per chunk.

    Accepts a stream of ChunkRequest frames (typically one) and for each request
    yields one ChunkItem per chunk. Uses character-level splitting with
    configurable chunk_size and overlap. Defaults to chunk_size=1000 and
    overlap=200 when the input fields are zero. Chunks are trimmed of
    leading/trailing whitespace and empty chunks are dropped. Each ChunkItem
    carries the chunk text, source filename, and its index within the total
    chunk count so downstream nodes can track provenance.
    """
    for input in inputs:
        chunk_size = input.chunk_size if input.chunk_size > 0 else DEFAULT_CHUNK_SIZE
        overlap = input.overlap if input.overlap >= 0 else DEFAULT_OVERLAP

        if overlap >= chunk_size:
            log.warn("text_chunker: overlap >= chunk_size, clamping overlap", chunk_size=chunk_size, overlap=overlap)
            overlap = chunk_size // 4

        text = input.text
        step = chunk_size - overlap
        raw_chunks = []
        start = 0
        while start < len(text):
            end = start + chunk_size
            chunk = text[start:end].strip()
            if chunk:
                raw_chunks.append(chunk)
            start += step

        total = len(raw_chunks)
        log.info("text_chunker: streaming chunks", chunk_count=total, chunk_size=chunk_size, overlap=overlap)

        for i, chunk_text in enumerate(raw_chunks):
            yield ChunkItem(
                text=chunk_text,
                source_filename=input.source_filename,
                chunk_index=i,
                total_chunks=total,
            )
