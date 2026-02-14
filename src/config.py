import os

GEOAPIFY_API_KEY = os.getenv("GEOAPIFY_API_KEY")
GEOAPIFY_AUTOCOMPLETE_URL = "https://api.geoapify.com/v1/geocode/autocomplete"

if GEOAPIFY_API_KEY is None:
    # Optional: raise early or log a warning
    raise RuntimeError(
        "GEOAPIFY_API_KEY environment variable is not set."
    )