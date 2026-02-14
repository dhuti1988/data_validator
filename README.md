## data_validator

Utility package for validating and transforming data, including:

- **PII masking**: functions to hide or partially mask emails, phone numbers, etc.
- **Address validation**: integrates with the Geoapify autocomplete API to validate and normalize addresses.

### Quickstart

Install deps:

```bash
python3 -m pip install -r requirements.txt
```

Run tests:

```bash
python3 -m pytest -q
```

### Environment variables

- **`GEOAPIFY_API_KEY`**: required at runtime for `src/utils/address_utils.py` (Geoapify autocomplete).

### Project structure

data_validator/
  README.md
  requirements.txt
  src/
    config.py                # Geoapify config (reads GEOAPIFY_API_KEY)
    utils/
      pii_utils.py            # PII masking utilities
      address_utils.py        # Address validation via Geoapify
      spark_utils.py          # PySpark helpers (mask/enrich DataFrames)
      data_generator_utils.py # Sample data generation (S3)
  tests/
    conftest.py
    test_pii_utils.py
    test_address_utils.py
    test_spark_utils.py

### CI / GitHub Actions (S3 upload)

The workflow `.github/workflows/build_and_publish.yml`:

- runs `pytest`
- if tests pass, zips `src/` and uploads `src_<git sha>.zip` to S3

Required GitHub **repo secrets** (Repo → Settings → Secrets and variables → Actions):

- **`AWS_ACCESS_KEY_ID`**
- **`AWS_SECRET_ACCESS_KEY`**
- **`AWS_REGION`** (e.g. `us-east-1`)
- **`S3_BUCKET`** (bucket name)
- **`S3_PREFIX`** (optional prefix/folder)
