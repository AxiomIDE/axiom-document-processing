from gen.messages_pb2 import ChunkRequest, ChunkItem
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
    results = list(text_chunker(log, secrets, iter([req])))
    assert len(results) > 1
    for i, item in enumerate(results):
        assert isinstance(item, ChunkItem)
        assert item.source_filename == "doc.pdf"
        assert item.chunk_index == i
        assert item.total_chunks == len(results)
        assert len(item.text) <= 1000


def test_text_chunker_uses_defaults_when_zero():
    log = _NoOpLogger()
    secrets = _NoOpSecrets()
    text = "b" * 3000
    req = ChunkRequest(text=text, chunk_size=0, overlap=0, source_filename="")
    results = list(text_chunker(log, secrets, iter([req])))
    assert len(results) > 1
    for item in results:
        assert isinstance(item, ChunkItem)
