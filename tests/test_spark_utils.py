import pytest


def test_mask_all_pii_in_df_partial_masks_email_and_phone(spark):
    from src.utils.pii_utils import mask_email, mask_phone

    df = spark.createDataFrame(
        [
            {
                "name": "John",
                "email": "john.doe@example.com",
                "phone": "+1 555-123-4567",
                "address": "1600 Amphitheatre Pkwy, Mountain View",
            }
        ]
    )

    from src.utils.spark_utils import mask_all_pii_in_df

    out = mask_all_pii_in_df(df, strategy="partial").collect()[0].asDict()
    assert out["email"] == mask_email("john.doe@example.com", strategy="partial")
    assert out["phone"] == mask_phone("+1 555-123-4567", strategy="partial")


def test_mask_all_pii_in_df_no_email_or_phone_columns_noop(spark):
    df = spark.createDataFrame([{"name": "OnlyName"}])

    from src.utils.spark_utils import mask_all_pii_in_df

    out = mask_all_pii_in_df(df, strategy="partial").collect()[0].asDict()
    assert out == {"name": "OnlyName"}


def test_enrich_address_in_df_adds_validated_column(monkeypatch, spark):
    from pyspark.sql.functions import col

    df = spark.createDataFrame(
        [{"address": "1600 Amphitheatre Pkwy, Mountain View", "name": "John"}]
    )

    # Avoid real HTTP by stubbing the function used by the UDF.
    import src.utils.spark_utils as spark_utils

    monkeypatch.setattr(spark_utils, "validate_address", lambda v: f"VALIDATED({v})")

    out_df = spark_utils.enrich_address_in_df(df, address_column="address")
    row = out_df.select(col("address_validated")).collect()[0]
    assert row["address_validated"] == "VALIDATED(1600 Amphitheatre Pkwy, Mountain View)"
