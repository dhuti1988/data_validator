"""
Address validation utilities using Geoapify API.

This module provides functions to validate and enrich address strings
by normalizing them using the Geoapify autocomplete service.
"""
import requests
from src.config import GEOAPIFY_API_KEY, GEOAPIFY_AUTOCOMPLETE_URL


class AddressValidationError(Exception):
    """Raised when address validation fails."""

def validate_address(query: str) -> str:
    """
    Validate and enrich an address string using Geoapify autocomplete.
    """
    if not query:
        raise ValueError("query must be a non-empty string")

    # Prepare API request parameters
    params = {
        "text": query,
        "apiKey": GEOAPIFY_API_KEY,
        "limit": 1,  # Only need the first/best match
    }

    try:
        # Make API request
        resp = requests.get(GEOAPIFY_AUTOCOMPLETE_URL, params=params, timeout=5)
    except requests.RequestException as exc:
        raise AddressValidationError(f"Request error: {exc}") from exc

    # Check for HTTP errors
    if resp.status_code != 200:
        raise AddressValidationError(
            f"Geoapify returned status {resp.status_code}: {resp.text[:200]}"
        )

    # Parse API response
    api_response = resp.json()
    features = api_response.get("features", [])
    
    if not features:
        raise AddressValidationError("No address matches found")

    # Extract address properties from the first (best) match
    new_data = features[0]["properties"]
    
    # Build enriched address dictionary
    address_enriched = [
        new_data.get("address_line1", ""),
        new_data.get("city", ""),
        new_data.get("state_code", ""),
        new_data.get("postcode", "")
    ]
    
    return ' '.join(address_enriched)