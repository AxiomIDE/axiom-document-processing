from gen.messages_pb2 import ChunkRequest, ChunkedDocument
from nodes.text_chunker import text_chunker


class _NoOpLogger:
    """Minimal AxiomLogger implementation for unit tests."""
    def debug(self, msg: str, **attrs) -> None: pass
    def info(self, msg: str, **attrs) -> None: pass
    def warn(self, msg: str, **attrs) -> None: pass
    def error(self, msg: str, **attrs) -> None: pass


class _NoOpSecrets:
    def get(self, name: str):
        return "", False


def test_text_chunker_basic_split():
    log = _NoOpLogger()
    secrets = _NoOpSecrets()
    text = "a" * 2500
    req = ChunkRequest(text=text, chunk_size=1000, overlap=200, source_filename="doc.pdf")
    result = text_chunker(log, secrets, req)
    assert isinstance(result, ChunkedDocument)
    assert result.source_filename == "doc.pdf"
    assert len(result.chunks) > 1
    assert all(len(c) <= 1000 for c in result.chunks)


def test_text_chunker_uses_defaults_when_zero():
    log = _NoOpLogger()
    secrets = _NoOpSecrets()
    text = "b" * 3000
    req = ChunkRequest(text=text, chunk_size=0, overlap=0, source_filename="")
    result = text_chunker(log, secrets, req)
    assert len(result.chunks) > 1
