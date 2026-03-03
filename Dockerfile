# start from a Python base with OpenJDK for Spark
FROM python:3.12-slim

# install only Java runtime required by Spark
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
           openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# set java home for Spark
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# copy only requirements first to leverage Docker cache
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# copy only runtime source files (avoid copying git history, caches, datasets)
COPY main.py pipeline.py ./
COPY src ./src

# create directories for data to avoid permission issues
RUN mkdir -p Sample_data/raw Sample_data/processed Sample_data/reports

# use non-root user for safety
RUN useradd -m app && chown -R app:app /app
USER app

# default command runs the ETL job
CMD ["python", "main.py"]