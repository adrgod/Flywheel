import os
import sys
from src.utils.spark_utils import get_spark_session
from pipeline import process_data, analytic_calculations

def main():
    # 1. Initialize Spark (local mode)
    spark = get_spark_session(app_name="JSON_Inconsistent_ETL")
    
    # 2. Define Paths
    # Tip: Use absolute paths to avoid issues with local execution
    base_path = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(base_path, "Sample_data/raw/")
    output_path = os.path.join(base_path, "Sample_data/processed/")

    print(f"[*] Starting ETL Job...")
    print(f"[*] Reading files from: {input_path}")

    try:
        # 3. Check if input directory has files
        if not os.path.exists(input_path) or not os.listdir(input_path):
            print(f"[!] Warning: Input directory {input_path} is empty or missing.")
            return

        # 4. Execute the ETL Logic
        # This handles the inconsistent JSON parsing and deduplication
        df = process_data(spark, input_path, output_path)
        
        print(f"[+] ETL Successful! Partitioned data saved to: {output_path}")

        analytic_calculations(base_path)

    except Exception as e:
        print(f"[X] ETL Failed: {str(e)}")
        sys.exit(1)
    
    finally:
        # 5. Always stop the Spark session to free up local memory
        spark.stop()

if __name__ == "__main__":
    main()
