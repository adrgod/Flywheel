# Flywheel

This repository provides a lightweight Spark-based ETL pipeline for
processing inconsistent JSON data from multiple vendors.  It is written in
Python and uses PySpark, so both a Java runtime and the correct Python
environment are required.

---

## 1. Local setup (Python)

1. **Python environment**

   The project has a conda environment specification (`environment.yml`), but
   you may also use `requirements.txt` with `pip`.  Example with conda:

   ```bash
   conda env create -n flywheel -f environment.yml
   conda activate flywheel
   ```

   If you prefer `pip`:

   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Java requirement**

   Spark requires Java 11+.  On macOS install via Homebrew and export
   `JAVA_HOME`:

   ```bash
   brew install openjdk
   export JAVA_HOME="$(/usr/libexec/java_home)"
   ```

3. **Run the pipeline**

   With the environment activated, execute:

   ```bash
   python main.py
   ```

   Ensure you run from the project root so that the relative `data/`
   directories resolve correctly.

4. **Tests**

   ```bash
   python -m pytest
   ```

---

## 2. Docker

A `Dockerfile` is provided for containerized execution.  It installs Python,
Java (for Spark) and the required Python packages.

**Build the image:**

```bash
# include the current directory (`.`) as the build context
# the previous example was missing the final "." which causes an error
docker build -t flywheel-etl .
```

**Run the container:**

```bash
# bind-mount a directory from your local machine so that files written
# inside the container appear on your host.
# you can use any path, just make sure the left side exists.

docker run --rm -v "$(pwd)/Sample_data:/app/Sample_data" flywheel-etl

# for example, if you want output in ~/my-results, create that directory and
# mount it:
# docker run --rm -v "${HOME}/my-results:/app/data/processed" flywheel-etl

# now anything the container writes to /app/data/processed will show up in
# the host folder (e.g. $(pwd)/data/processed or ~/my-results).```

By default the container runs `python main.py`; your mounted `data` folder will
be visible under `/app/data` inside the container.  Output written to
`data/processed` on the container appears in the host `data/processed` due to
the bind mount.

To override paths you can add environment variables (not currently read by the
code, but shown here for illustration):

```bash
docker run --rm -v "$(pwd)/Sample_data:/app/Sample_data" \
    -e INPUT_PATH=/app/Sample_data/raw -e OUTPUT_PATH=/app/Sample_data/processed \
    flywheel-etl
```

---

## 3. Troubleshooting

- `ModuleNotFoundError: No module named 'pyspark'`: make sure you're
  running inside the correct Python environment and that `pyspark` is
  installed (`pip install pyspark`).
- `Unable to locate a Java Runtime` /
  `[JAVA_GATEWAY_EXITED] Java gateway process exited before sending its port number`:
  install Java and set `JAVA_HOME` as shown above.

---

## 4. Project structure

```
AI_PROMPTS.md
DESIGN.md
main.py
pipeline.py
requirements.txt
SOLUTION.md
Sample_data/
src/
  utils/spark_utils.py
tests/
```
