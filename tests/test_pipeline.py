import json
from pathlib import Path

import pandas as pd

from pipeline import _parse_date, _parse_timestamp, analytic_calculations, process_data


def test_parse_date(spark):
    rows = [
        ("2024-01-15",),
        ("2024/01/16",),
        ("01/17/2024",),
        ("01-18-2024",),
        ("01/19/24",),
        ("invalid-date",),
    ]
    df = spark.createDataFrame(rows, ["raw_date"])
    parsed = df.select(_parse_date(df.raw_date).alias("parsed")).collect()
    parsed_values = [r[0].isoformat() if r[0] else None for r in parsed]

    assert parsed_values == [
        "2024-01-15",
        "2024-01-16",
        "2024-01-17",
        "2024-01-18",
        "2024-01-19",
        None,
    ]


def test_parse_timestamp(spark):
    rows = [
        ("2024-01-15T08:30:00Z",),
        ("2024-01-15 08:45:00",),
        ("2024-01-15",),
        ("invalid-ts",),
    ]
    df = spark.createDataFrame(rows, ["raw_ts"])
    parsed = df.select(_parse_timestamp(df.raw_ts).alias("parsed")).collect()

    assert parsed[0][0] is not None
    assert parsed[1][0] is not None
    assert parsed[2][0] is not None
    assert parsed[3][0] is None


def test_process_data_duplicated(spark, input_output_paths):
    input_path, output_path = input_output_paths
    input_dir = Path(input_path)

    vendor_a = [
        {
            "campaign_id": "camp_001",
            "campaign_name": "Summer Sale",
            "vendor_timestamp": "2024-01-15T08:30:00Z",
            "platform": "Meta",
            "metrics": {
                "impressions": 1000,
                "clicks": 100,
                "conversions": 5,
                "spend": 12.5,
            },
            "audience": {"region": "US", "age_group": "25-34"},
            "status": "active",
        },
        {
            "campaign_id": "camp_001",
            "campaign_name": "Summer Sale",
            "vendor_timestamp": "2024-01-15T08:30:00Z",
            "platform": "Meta",
            "metrics": {
                "impressions": 1000,
                "clicks": 100,
                "conversions": 5,
                "spend": 12.5,
            },
            "audience": {"region": "US", "age_group": "25-34"},
            "status": "active",
        },
    ]

    vendor_b_csv = """ad_network_id,network_name,region,impressions,clicks,spend_usd,report_date
net_01,Google,US,5000,230,33.4,2024-01-15
"""

    (input_dir / "vendor-a.json").write_text(json.dumps(vendor_a), encoding="utf-8")
    (input_dir / "vendor-b.csv").write_text(vendor_b_csv, encoding="utf-8")

    process_data(spark, input_path, output_path)

    result = spark.read.parquet(output_path)
    assert result.count() == 2

    expected_columns = {
        "id",
        "vendor",
        "name",
        "platform",
        "status",
        "region",
        "age_group",
        "impressions",
        "clicks",
        "conversions",
        "spend",
        "event_timestamp",
        "date",
        "date_str",
        "etl_load_utc_ts",
    }
    assert expected_columns.issubset(set(result.columns))

    vendors = {r[0] for r in result.select("vendor").distinct().collect()}
    assert vendors == {"vendor_a", "vendor_b"}

    partition_dirs = [p.name for p in Path(output_path).iterdir() if p.is_dir()]
    assert any(name.startswith("date_str=") for name in partition_dirs)


def test_analytic_calculations(tmp_path):
    base_path = Path(tmp_path)
    processed_dir = base_path / "Sample_data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(
        [
            {
                "id": "camp_001",
                "vendor": "vendor_a",
                "name": "Campaign A",
                "spend": 10.5,
                "clicks": 2,
                "impressions": 100,
                "conversions": 1,
                "date_str": "2024-01-15",
            },
            {
                "id": "camp_002",
                "vendor": "vendor_b",
                "name": "Campaign B",
                "spend": 22.0,
                "clicks": 5,
                "impressions": 300,
                "conversions": 2,
                "date_str": "2024-01-15",
            },
        ]
    )
    df.to_parquet(processed_dir / "part-00000.parquet", index=False)

    analytic_calculations(str(base_path))

    reports_dir = base_path / "Sample_data" / "reports"
    assert (reports_dir / "totals_by_vendor.csv").exists()
    assert (reports_dir / "totals_by_campaign.csv").exists()
    assert (reports_dir / "totals_by_day_vendor.csv").exists()
