import os
import sys
import logging
from src.utils.spark_utils import get_spark_session
from pipeline import process_data, analytic_calculations


def _setup_logger(base_path: str) -> logging.Logger:
    log_path = os.getenv("LOG_PATH", os.path.join(base_path, "Sample_data/logs/etl.log"))
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    logger = logging.getLogger("flywheel_etl")
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger

def main():
    base_path = os.path.dirname(os.path.abspath(__file__))
    logger = _setup_logger(base_path)

    # 1. Initialize Spark (local mode)
    spark = get_spark_session(app_name="JSON_Inconsistent_ETL")
    
    # 2. Define Paths
    input_path = os.getenv("INPUT_PATH", os.path.join(base_path, "Sample_data/raw/"))
    output_path = os.getenv("OUTPUT_PATH", os.path.join(base_path, "Sample_data/processed/"))

    print(f"[*] Starting ETL Job...")
    print(f"[*] Reading files from: {input_path}")
    logger.info("Starting ETL job")
    logger.info("Reading files from: %s", input_path)

    try:
        # 3. Check if input directory has files
        if not os.path.exists(input_path) or not os.listdir(input_path):
            logger.warning("Input directory %s is empty or missing", input_path)
            return

        # 4. Execute the ETL Logic
        process_data(spark, input_path, output_path, logger=logger)
        
        logger.info("ETL successful. Partitioned data saved to: %s", output_path)

        analytic_calculations(base_path, logger=logger)
        logger.info("Analytics reports generated under Sample_data/reports")

    except Exception as e:
        logger.exception("ETL failed: %s", str(e))
        sys.exit(1)
    
    finally:
        # 5. Always stop the Spark session to free up local memory
        spark.stop()
        logger.info("Spark session stopped")
        logger.info("ETL finished")

if __name__ == "__main__":
    main()
