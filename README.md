# Flywheel

## How to use

This ETL uses **PySpark**, which requires **Java 17**.

Please use a conda env, reading the yaml file to have all running.

```bash
conda env create -n flywheel -f environment.yml
conda activate flywheel
python main.py
```
# Flywheel

Work scenario to manage unscructured source files

1. **Running it**
to execute the code, main.py is the starting point so run:

   ```bash
   python main.py
   ```

2. **Testing it**
run the tests for the scenarios created

   ```bash
   python -m pytest
   ```
