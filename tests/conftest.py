import importlib
import os
import sys
from pathlib import Path

import pytest


# Ensure project root is on sys.path so `import src...` works under pytest.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# `src/config.py` raises at import time if this env var is missing.
# Set a dummy value for tests so importing `src.utils.address_utils` / `src.utils.spark_utils`
# doesn't crash during collection.
os.environ.setdefault("GEOAPIFY_API_KEY", "test_dummy_key")

# If `src.config` was already imported for some reason, reload it after setting env.
if "src.config" in sys.modules:
    importlib.reload(sys.modules["src.config"])


@pytest.fixture(scope="session")
def spark():
    """
    Lightweight local SparkSession for unit-ish tests.
    Skips gracefully if Spark/Java isn't available in the environment.
    """
    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    try:
        session = (
            SparkSession.builder.master("local[1]")
            .appName("data_validator_pytest")
            .getOrCreate()
        )
        session.sparkContext.setLogLevel("ERROR")
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"Spark not available: {exc}")

    yield session

    session.stop()
