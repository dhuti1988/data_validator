import re


def test_mask_email_none_returns_none():
    from src.utils.pii_utils import mask_email

    assert mask_email(None) is None


def test_mask_email_no_email_returns_original():
    from src.utils.pii_utils import mask_email

    value = "hello world"
    assert mask_email(value) == value


def test_mask_email_partial_masks_local_part():
    from src.utils.pii_utils import mask_email

    assert mask_email("john.doe@example.com", strategy="partial") == "j***@example.com"


def test_mask_email_full_masks_local_part_entirely():
    from src.utils.pii_utils import mask_email

    assert mask_email("john.doe@example.com", strategy="full") == "***@example.com"


def test_mask_email_inside_text_only_replaces_match():
    from src.utils.pii_utils import mask_email

    value = "Contact: john.doe@example.com ASAP"
    assert mask_email(value, strategy="partial") == "Contact: j***@example.com ASAP"


def test_mask_phone_none_returns_none():
    from src.utils.pii_utils import mask_phone

    assert mask_phone(None) is None


def test_mask_phone_no_phone_returns_original():
    from src.utils.pii_utils import mask_phone

    value = "no phone here"
    assert mask_phone(value) == value


def test_mask_phone_partial_keeps_last_digits_and_masks_rest():
    from src.utils.pii_utils import mask_phone

    masked = mask_phone("Call me at +1 555-123-4567", strategy="partial")

    # For this input, digits are "15551234567" so output is only "*" and trailing digits.
    assert re.search(r"\*+\d{2}$", masked) is not None
    assert "4567" not in masked  # implementation keeps only last 2 here


def test_mask_pii_masks_email_and_phone_in_text():
    from src.utils.pii_utils import mask_pii

    value = "Email john.doe@example.com or call +1 555-123-4567."
    masked = mask_pii(value)
    assert "john.doe@example.com" not in masked
    assert "j***@example.com" in masked
    assert "+1 555-123-4567" not in masked
