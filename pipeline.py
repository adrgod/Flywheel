import os

from pyspark.sql import functions as F
from pyspark.sql.types import StructType
import pandas as pd


def _parse_date(col: F.Column) -> F.Column:
    col_as_str = col.cast("string")
    return F.coalesce(
        F.when(col_as_str.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(col_as_str, "yyyy-MM-dd")),
        F.when(col_as_str.rlike(r"^\d{4}/\d{2}/\d{2}$"), F.to_date(col_as_str, "yyyy/MM/dd")),
        F.when(col_as_str.rlike(r"^\d{2}/\d{2}/\d{4}$"), F.to_date(col_as_str, "MM/dd/yyyy")),
        F.when(col_as_str.rlike(r"^\d{2}-\d{2}-\d{4}$"), F.to_date(col_as_str, "MM-dd-yyyy")),
        F.when(col_as_str.rlike(r"^\d{2}/\d{2}/\d{2}$"), F.to_date(col_as_str, "MM/dd/yy")),
    )


def _parse_timestamp(col: F.Column) -> F.Column:
    col_as_str = col.cast("string")
    return F.coalesce(
        F.when(
            col_as_str.rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:?\d{2})$"),
            F.to_timestamp(col_as_str, "yyyy-MM-dd'T'HH:mm:ssX"),
        ),
        F.when(
            col_as_str.rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"),
            F.to_timestamp(col_as_str, "yyyy-MM-dd HH:mm:ss"),
        ),
        F.when(
            col_as_str.rlike(r"^\d{4}-\d{2}-\d{2}$"),
            F.to_timestamp(col_as_str, "yyyy-MM-dd"),
        ),
    )


def _raise_if_invalid_format(df, raw_col_name: str, parsed_col: F.Column, label: str) -> None:
    if raw_col_name not in df.columns:
        return

    invalid_df = df.where(F.col(raw_col_name).isNotNull() & parsed_col.isNull())
    invalid_count = invalid_df.count()
    if invalid_count > 0:
        invalid_samples = [
            row[0]
            for row in invalid_df.select(raw_col_name).distinct().limit(5).collect()
            if row[0] is not None
        ]
        raise ValueError(
            f"Invalid {label} format in column '{raw_col_name}'. "
            f"Found {invalid_count} invalid row(s). "
            f"Examples: {invalid_samples}"
        )


def process_data(spark, input_path, output_path):
    """
    Reads data from both vendors, parses, partitions it and writes to output path.
    Source and Output should then be a S3 location
    """

    # Vendor A (JSON)
    df_json_raw = (
        spark.read.option("multiline", "true")
        .option("pathGlobFilter", "*.json")
        .json(input_path)
    )

    ts_a = _parse_timestamp(F.col("vendor_timestamp"))
    _raise_if_invalid_format(df_json_raw, "vendor_timestamp", ts_a, "timestamp")
    date_a = F.to_date(ts_a)

    # Pre-compute expressions so schema changes don't break the pipeline.
    if "campaign_id" in df_json_raw.columns:
        id_expr = F.col("campaign_id").cast("string").alias("id")
    else:
        id_expr = F.lit(None).cast("string").alias("id")

    # We always know this is vendor A; if you really want to depend on a column,
    # you can switch this to a conditional too.
    vendor_expr = F.lit("vendor_a").alias("vendor")

    if "campaign_name" in df_json_raw.columns:
        name_expr = F.col("campaign_name").cast("string").alias("name")
    else:
        name_expr = F.lit(None).cast("string").alias("name")

    if "platform" in df_json_raw.columns:
        platform_expr = F.col("platform").cast("string").alias("platform")
    else:
        platform_expr = F.lit(None).cast("string").alias("platform")

    if "status" in df_json_raw.columns:
        status_expr = F.col("status").cast("string").alias("status")
    else:
        status_expr = F.lit(None).cast("string").alias("status")

    if "audience" in df_json_raw.columns:
        region_expr = F.col("audience.region").cast("string").alias("region")
        age_group_expr = F.col("audience.age_group").cast("string").alias("age_group")
    else:
        region_expr = F.lit(None).cast("string").alias("region")
        age_group_expr = F.lit(None).cast("string").alias("age_group")

    if "metrics" in df_json_raw.columns:
        impressions_expr = F.col("metrics.impressions").cast("long").alias("impressions")
        clicks_expr = F.col("metrics.clicks").cast("long").alias("clicks")
        conversions_expr = F.col("metrics.conversions").cast("long").alias("conversions")
        spend_expr = F.col("metrics.spend").cast("double").alias("spend")
    else:
        impressions_expr = F.lit(None).cast("long").alias("impressions")
        clicks_expr = F.lit(None).cast("long").alias("clicks")
        conversions_expr = F.lit(None).cast("long").alias("conversions")
        spend_expr = F.lit(None).cast("double").alias("spend")

    ts_expr = F.date_format(ts_a, "yyyy-MM-dd HH:mm:ss").alias("event_timestamp")

    df_a = df_json_raw.select(
        id_expr,
        vendor_expr,
        name_expr,
        platform_expr,
        status_expr,
        region_expr,
        age_group_expr,
        impressions_expr,
        clicks_expr,
        conversions_expr,
        spend_expr,
        ts_expr,
        date_a.alias("date"),
    )

    # Vendor B (CSV)
    df_csv_raw = (
        spark.read.option("header", "true")
        .option("multiLine", "true")
        .option("escape", "\"")
        .option("pathGlobFilter", "*.csv")
        .csv(input_path)
    )

    date_b = _parse_date(F.col("report_date"))
    _raise_if_invalid_format(df_csv_raw, "report_date", date_b, "date")
    ts_b = F.to_timestamp(date_b)

    df_b = df_csv_raw.select(
        F.col("ad_network_id").cast("string").alias("id"),
        F.lit("vendor_b").alias("vendor"), #adding the name of the vendor
        F.col("network_name").cast("string").alias("name"),
        F.col("network_name").cast("string").alias("platform"),
        F.lit(None).cast("string").alias("status"),
        F.col("region").cast("string").alias("region"),
        F.lit(None).cast("string").alias("age_group"),
        F.col("impressions").cast("long").alias("impressions"),
        F.col("clicks").cast("long").alias("clicks"),
        F.lit(None).cast("long").alias("conversions"),
        F.col("spend_usd").cast("double").alias("spend"),
        ts_b.alias("event_timestamp"),
        date_b.alias("date")
    )

    df_all = df_a.unionByName(df_b, allowMissingColumns=True)
    
    #date validation flag to filter out or to count missed data
    df_all = df_all.withColumn(
    "date_str",
    F.when(F.col("date").isNull(),
           F.lit("invalid_date"))
     .otherwise(F.date_format("date", "yyyy-MM-dd"))
    )

    df_all = df_all.withColumn(
        "etl_load_utc_ts",
        F.date_format(
            F.to_utc_timestamp(F.current_timestamp(), "UTC"),
            "yyyy-MM-dd HH:mm:ss"
        )
    )

    df_cleaned = df_all.dropDuplicates(["vendor", "id", "event_timestamp"])

    (
        df_cleaned.write.mode("overwrite")
        .partitionBy("date_str") #partitioning by date, including all vendors
        .option("header", "true")
        .parquet(output_path)
    )

def analytic_calculations(base_path):

    output_path = os.path.join(base_path, "Sample_data/processed")

    df_cleaned = pd.read_parquet(output_path, engine="pyarrow")  # reads partitioned parquet folder

    #removing campaigns that have not data about number of clicks
    df_cleaned = df_cleaned[
        df_cleaned["clicks"].notna()
    ]

    df_cleaned["spend"] = pd.to_numeric(df_cleaned["spend"], errors="coerce").fillna(0.0)
    df_cleaned["clicks"] = pd.to_numeric(df_cleaned["clicks"], errors="coerce").fillna(0).astype("int64")
    df_cleaned["impressions"] = pd.to_numeric(df_cleaned["impressions"], errors="coerce").fillna(0).astype("int64")
    df_cleaned["conversions"] = pd.to_numeric(df_cleaned["conversions"], errors="coerce").fillna(0).astype("int64")

    totals_by_vendor = (
        df_cleaned.groupby("vendor", dropna=False, observed=False)
        .agg(total_spend=("spend", "sum"),
            total_clicks=("clicks", "sum"),
            campaigns=("id", "nunique"))
        .reset_index()
        .sort_values(["total_spend", "total_clicks"], ascending=False)
    )

    totals_by_campaign = (
        df_cleaned.groupby(["id", "name"], dropna=False, observed=False)
        .agg(total_spend=("spend", "sum"),
            total_clicks=("clicks", "sum"),
            rows=("id", "size"))
        .reset_index()
        .sort_values(["id", "total_spend"], ascending=[True, False])
    )
    
    totals_by_day_vendor = (
    df_cleaned.groupby(["date_str", "vendor"], dropna=False, observed=False)
    .agg(total_spend=("spend", "sum"),
        total_clicks=("clicks", "sum"))
    .reset_index()
    )

    # Save reports
    reports_dir = os.path.join(base_path, "Sample_data/reports")
    os.makedirs(reports_dir, exist_ok=True)

    totals_by_vendor.to_csv(os.path.join(reports_dir, "totals_by_vendor.csv"), index=False)
    totals_by_campaign.to_csv(os.path.join(reports_dir, "totals_by_campaign.csv"), index=False)
    totals_by_day_vendor.to_csv(os.path.join(reports_dir, "totals_by_day_vendor.csv"), index=False)



