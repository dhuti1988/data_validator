# ======================================================
# src/decrypt_utils.py
# ======================================================
import base64
import gzip
import io


def encode_base64(text: str) -> str:
    return base64.b64encode(text.encode()).decode()


def decode_base64(text: str) -> str:
    return base64.b64decode(text.encode()).decode()


def gzip_compress(text: str) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as f:
        f.write(text.encode())
    return buf.getvalue()


def gzip_decompress(data: bytes) -> str:
    with gzip.GzipFile(fileobj=io.BytesIO(data), mode="rb") as f:
        return f.read().decode()