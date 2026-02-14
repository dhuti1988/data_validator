import pytest


def test_validate_address_requires_non_empty_query():
    from src.utils.address_utils import validate_address

    with pytest.raises(ValueError):
        validate_address("")


def test_validate_address_wraps_request_errors(monkeypatch):
    import requests
    from src.utils.address_utils import AddressValidationError, validate_address

    def _boom(*args, **kwargs):
        raise requests.RequestException("network down")

    monkeypatch.setattr(requests, "get", _boom)

    with pytest.raises(AddressValidationError, match="Request error"):
        validate_address("1600 Amphitheatre Pkwy")


def test_validate_address_raises_on_non_200(monkeypatch):
    import requests
    from src.utils.address_utils import AddressValidationError, validate_address

    class _Resp:
        status_code = 500
        text = "oops"

        def json(self):
            return {}

    monkeypatch.setattr(requests, "get", lambda *a, **k: _Resp())

    with pytest.raises(AddressValidationError, match=r"status 500"):
        validate_address("1600 Amphitheatre Pkwy")


def test_validate_address_raises_when_no_features(monkeypatch):
    import requests
    from src.utils.address_utils import AddressValidationError, validate_address

    class _Resp:
        status_code = 200
        text = "ok"

        def json(self):
            return {"features": []}

    monkeypatch.setattr(requests, "get", lambda *a, **k: _Resp())

    with pytest.raises(AddressValidationError, match="No address matches found"):
        validate_address("1600 Amphitheatre Pkwy")


def test_validate_address_returns_enriched_string(monkeypatch):
    import requests
    from src.utils.address_utils import validate_address

    class _Resp:
        status_code = 200
        text = "ok"

        def json(self):
            return {
                "features": [
                    {
                        "properties": {
                            "address_line1": "1600 Amphitheatre Pkwy",
                            "city": "Mountain View",
                            "state_code": "CA",
                            "postcode": "94043",
                        }
                    }
                ]
            }

    monkeypatch.setattr(requests, "get", lambda *a, **k: _Resp())

    assert (
        validate_address("1600 Amphitheatre Pkwy, Mountain View")
        == "1600 Amphitheatre Pkwy Mountain View CA 94043"
    )
