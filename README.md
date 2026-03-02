# Flywheel

## Quick start (recommended)

This ETL uses **PySpark**, which requires **Java 17**.

Create the Conda env (includes Java 17):

```bash
conda env create -n flywheel -f environment.yml
conda activate flywheel
python main.py
```

## If you’re using `requirements.txt` instead

Install Python deps:

```bash
pip install -r requirements.txt
```

Install Java 17 on macOS (Homebrew):

```bash
brew install --cask temurin@17
```

Then run with Java 17:

```bash
export JAVA_HOME="$(/usr/libexec/java_home -v 17)"
export PATH="$JAVA_HOME/bin:$PATH"
python main.py
```
# Flywheel

This repository contains a simple Spark-based ETL pipeline for dealing with
inconsistent JSON data.  

## Setup Instructions

1. **Python environment**

   This project uses a conda environment (`venv_p314`) managed by VS Code.  
   Before running any scripts, activate the environment or use the provided  
   interpreter path:

   ```bash
   conda activate venv_p314
   # or
   /Users/AGN/Tools/miniconda/miniconda3/envs/venv_p314/bin/python main.py
   ```

   Make sure `pyspark` (and other dependencies listed in `requirements.txt`)  
   are installed in that environment:

   ```bash
   pip install -r requirements.txt
   ```

2. **Java requirement**

   Apache Spark requires a Java runtime.  Install Java 11+ and set `JAVA_HOME`  
   appropriately.  On macOS you can use Homebrew:

   ```bash
   brew install openjdk
   export JAVA_HOME="$(/usr/libexec/java_home)"
   ```

   Without Java you may see errors like ``Unable to locate a Java Runtime``  
   or ``Java gateway process exited before sending its port number``.

3. **Running the pipeline**

   After environment and Java are configured, execute the ETL job:

   ```bash
   python main.py
   ```

   Ensure your current directory is the project root so relative paths resolve.
