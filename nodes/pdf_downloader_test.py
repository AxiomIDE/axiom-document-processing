from gen.messages_pb2 import DownloadRequest, PDFInput
from nodes.pdf_downloader import pdf_downloader


class _NoOpLogger:
    def debug(self, msg: str, **attrs) -> None: pass
    def info(self, msg: str, **attrs) -> None: pass
    def warn(self, msg: str, **attrs) -> None: pass
    def error(self, msg: str, **attrs) -> None: pass


class _NoOpSecrets:
    def get(self, name: str):
        return "", False


def test_pdf_downloader_derives_filename_from_url():
    """Filename is derived from URL path when the optional field is absent."""
    import io
    import urllib.request
    from unittest.mock import patch, MagicMock

    fake_pdf = b"%PDF-1.4 fake content"
    mock_response = MagicMock()
    mock_response.read.return_value = fake_pdf
    mock_response.__enter__ = lambda s: s
    mock_response.__exit__ = MagicMock(return_value=False)

    with patch("urllib.request.urlopen", return_value=mock_response):
        result = pdf_downloader(_NoOpLogger(), _NoOpSecrets(),
                                DownloadRequest(url="https://arxiv.org/pdf/1706.03762"))

    assert isinstance(result, PDFInput)
    assert result.pdf_bytes == fake_pdf
    assert result.filename == "1706.03762"


def test_pdf_downloader_uses_explicit_filename():
    """Explicit filename takes precedence over URL-derived one."""
    import urllib.request
    from unittest.mock import patch, MagicMock

    fake_pdf = b"%PDF-1.4 fake content"
    mock_response = MagicMock()
    mock_response.read.return_value = fake_pdf
    mock_response.__enter__ = lambda s: s
    mock_response.__exit__ = MagicMock(return_value=False)

    with patch("urllib.request.urlopen", return_value=mock_response):
        result = pdf_downloader(_NoOpLogger(), _NoOpSecrets(),
                                DownloadRequest(url="https://arxiv.org/pdf/1706.03762",
                                                filename="attention.pdf"))

    assert result.filename == "attention.pdf"
    assert result.pdf_bytes == fake_pdf
