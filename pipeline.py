import os

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType
import pandas as pd


def _parse_date(col: F.Column) -> F.Column:
    return F.coalesce(
        F.to_date(col, "yyyy-MM-dd"),
        F.to_date(col, "yyyy/MM/dd"),
        F.to_date(col, "MM/dd/yyyy"),
        F.to_date(col, "MM-dd-yyyy"),
        F.to_date(col, "MM/dd/yy"),
    )


def _parse_timestamp(col: F.Column) -> F.Column:
    return F.coalesce(
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ssX"),
        F.to_timestamp(col, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(col, "yyyy-MM-dd"),
        F.to_timestamp(col, "MM-dd-yyyy"),
        F.to_timestamp(col, "dd-MM-yyyy"),
    )


def _flatten_json(df):
    """
    Flattens nested JSON columns recursively.
    - Struct fields are expanded to <parent>_<child> columns.
    - Array fields are exploded with explode_outer.
    """
    while True:
        struct_cols = [
            field.name
            for field in df.schema.fields
            if isinstance(field.dataType, StructType)
        ]
        array_cols = [
            field.name
            for field in df.schema.fields
            if isinstance(field.dataType, ArrayType)
        ]

        if not struct_cols and not array_cols:
            break

        for column_name in struct_cols:
            child_fields = df.schema[column_name].dataType.fields
            expanded = [
                F.col(f"{column_name}.{child.name}").alias(f"{column_name}_{child.name}")
                for child in child_fields
            ]
            df = df.select("*", *expanded).drop(column_name)

        for column_name in array_cols:
            df = df.withColumn(column_name, F.explode_outer(F.col(column_name)))

    return df


def _pick_first(df, candidates, cast_type):
    existing = [candidate for candidate in candidates if candidate in df.columns]
    if not existing:
        return F.lit(None).cast(cast_type)
    return F.coalesce(*[F.col(candidate).cast(cast_type) for candidate in existing])


def _pick_first_timestamp(df, candidates):
    existing = [candidate for candidate in candidates if candidate in df.columns]
    if not existing:
        return F.lit(None).cast("timestamp")
    return F.coalesce(*[_parse_timestamp(F.col(candidate).cast("string")) for candidate in existing])


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

    df_json_flat = _flatten_json(df_json_raw)

    ts_a = _pick_first_timestamp(df_json_flat, ["vendor_timestamp", "data_vendor_timestamp"])
    date_a = F.to_date(ts_a)

    # Pre-compute expressions so schema changes don't break the pipeline.
    id_expr = _pick_first(df_json_flat, ["campaign_id", "data_campaign_id"], "string").alias("id")

    # Adding field vendor with vendor's name
    vendor_expr = F.lit("vendor_a").alias("vendor")

    name_expr = _pick_first(df_json_flat, ["campaign_name", "data_campaign_name"], "string").alias("name")

    platform_expr = _pick_first(df_json_flat, ["platform", "data_platform"], "string").alias("platform")

    status_expr = _pick_first(df_json_flat, ["status", "data_status"], "string").alias("status")

    region_expr = _pick_first(
        df_json_flat,
        ["audience_region", "data_audience_region", "region", "data_region"],
        "string",
    ).alias("region")
    age_group_expr = _pick_first(
        df_json_flat,
        ["audience_age_group", "data_audience_age_group", "age_group", "data_age_group"],
        "string",
    ).alias("age_group")

    impressions_expr = _pick_first(
        df_json_flat,
        ["metrics_impressions", "data_metrics_impressions", "impressions", "data_impressions"],
        "long",
    ).alias("impressions")
    clicks_expr = _pick_first(
        df_json_flat,
        ["metrics_clicks", "data_metrics_clicks", "clicks", "data_clicks"],
        "long",
    ).alias("clicks")
    conversions_expr = _pick_first(
        df_json_flat,
        ["metrics_conversions", "data_metrics_conversions", "conversions", "data_conversions"],
        "long",
    ).alias("conversions")
    spend_expr = _pick_first(
        df_json_flat,
        ["metrics_spend", "data_metrics_spend", "spend", "data_spend"],
        "double",
    ).alias("spend")

    ts_expr = F.date_format(ts_a, "yyyy-MM-dd HH:mm:ss").alias("event_timestamp")

    df_a = df_json_flat.select(
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
    
    df_all = df_all.drop("date")

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

    count_of_invalid_dates = (
        df_cleaned.loc[
            df_cleaned["date_str"] == "invalid_date",
            ["vendor", "id", "name"]
        ]
        .groupby(["vendor", "id", "name"], dropna=False, observed=False)
        .size()
        .reset_index(name="rows")
        .sort_values(["vendor", "rows"], ascending=[True, False])
    )

    # Save reports
    reports_dir = os.path.join(base_path, "Sample_data/reports")
    os.makedirs(reports_dir, exist_ok=True)

    totals_by_vendor.to_csv(os.path.join(reports_dir, "totals_by_vendor.csv"), index=False)
    totals_by_campaign.to_csv(os.path.join(reports_dir, "totals_by_campaign.csv"), index=False)
    totals_by_day_vendor.to_csv(os.path.join(reports_dir, "totals_by_day_vendor.csv"), index=False)

    count_of_invalid_dates.to_csv(os.path.join(reports_dir, "count_of_invalid_dates.csv"), index=False)



