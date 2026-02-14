"""
PII (Personally Identifiable Information) masking utilities.

This module provides functions to mask or partially obscure sensitive information
like email addresses and phone numbers in text strings.
Also provides Spark UDFs for use in PySpark DataFrames.
"""
import re
from typing import Literal

# Type alias for masking strategy options
MaskStrategy = Literal["full", "partial"]

# Compiled regex pattern for matching email addresses
_EMAIL_RE = re.compile(r"(?P<local>[^@\s]+)@(?P<domain>[\w\.-]+)")

# Compiled regex pattern for matching phone numbers
_PHONE_RE = re.compile(r"\+?\d[\d\-\s]{7,}\d")


def mask_email(value: str, strategy: MaskStrategy = "partial") -> str:
    if value is None:
        return None
    match = _EMAIL_RE.search(value)
    if not match:
        return value
    local = match.group("local")
    domain = match.group("domain")
    if strategy == "full":
        masked = f"***@{domain}"
    else:  # partial
        visible = local[0] if local else ""
        masked = f"{visible}***@{domain}"
    return value.replace(match.group(0), masked)


def mask_phone(value: str, strategy: MaskStrategy = "partial") -> str:
    if value is None:
        return None
    def _mask(match: re.Match) -> str:
        digits = re.sub(r"\D", "", match.group(0))
        if strategy == "full":
            return "***" * max(1, len(digits) // 3)
        keep = min(4, max(2, len(digits) // 4))
        masked = "*" * (len(digits) - keep) + digits[-keep:]
        return masked
    return _PHONE_RE.sub(_mask, value)


def mask_pii(value: str) -> str:
    if value is None:
        return None
    value = mask_email(value, strategy="partial")
    value = mask_phone(value, strategy="partial")
    return value

