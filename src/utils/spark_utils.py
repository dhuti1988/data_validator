# ======================================================
# src/spark_utils.py
# ======================================================
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, count, when, current_timestamp, lit, udf
from src.utils.pii_utils import mask_email, mask_phone, mask_pii
from src.utils.address_utils import validate_address
from pyspark.sql.types import StringType

# UDFs (User Defined Functions) for PySpark
# Email masking UDFs
mask_email_partial_udf = udf(lambda v: mask_email(v, "partial"), StringType())
mask_email_full_udf = udf(lambda v: mask_email(v, "full"), StringType())

# Phone masking UDFs
mask_phone_partial_udf = udf(lambda v: mask_phone(v, "partial"), StringType())
mask_phone_full_udf = udf(lambda v: mask_phone(v, "full"), StringType())

validate_address_udf = udf(lambda v: validate_address(v), StringType())


def explode_json_array(df: DataFrame, col_name: str) -> DataFrame:
    return df.withColumn(col_name, explode(col(col_name)))


def get_null_counts(df: DataFrame) -> DataFrame:
    return df.select([
    count(when(col(c).isNull(), c)).alias(c + "_nulls")
    for c in df.columns
    ])


def add_audit_columns(df: DataFrame, source_name: str) -> DataFrame:
    return (
    df.withColumn("etl_ts", current_timestamp())
    .withColumn("source", lit(source_name))
    )

def mask_all_pii_in_df(df: DataFrame, strategy:str = "partial") -> DataFrame:
    # Loop through columns to check if there is an email or a phone
    all_columns = df.columns

    # if phone is one of the columns, then mask it
    if 'phone' in all_columns:
        if strategy == 'partial':
            df = df.withColumn('phone',mask_phone_partial_udf(col('phone')))
        if strategy == 'full':
            df = df.withColumn('phone',mask_phone_full_udf(col('phone')))

     # if email is one of the columns, then mask it
    if 'email' in all_columns:
        if strategy == 'partial':
            df = df.withColumn('email',mask_email_partial_udf(col('email')))
        if strategy == 'full':
            df = df.withColumn('email',mask_email_full_udf(col('email')))

    return df

def enrich_address_in_df(df: DataFrame, address_column:str = "address") -> DataFrame:
    validated_address_column_name = f"{address_column}_validated"
    df = df.withColumn(validated_address_column_name,validate_address_udf(col(address_column)))
    return df
