try:
    from pyspark.sql import SparkSession
except ImportError as e:
    # fail fast with clear guidance if pyspark isn't available in the current
    # Python environment. Users frequently run the script with the wrong
    # interpreter, which causes ModuleNotFoundError at import time.
    raise ImportError(
        "pyspark is required but not installed in this Python environment. "
        "Make sure you've activated the correct virtual/conda env (see the "
        "python environment details above) and then run `pip install pyspark`."
    ) from e

def get_spark_session(app_name="LocalETL"):
    return SparkSession.builder \
        .master("local[*]") \
        .appName(app_name) \
        .config("spark.sql.ansi.enabled", "false") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()