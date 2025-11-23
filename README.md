# ETL Data Lake Demo

PySpark pipeline that takes messy e-commerce data (customers, products, sales) and transforms it into a clean star schema ready for analytics.

## What it does

- Generates synthetic datasets with realistic data quality issues (nulls, duplicates, invalid FKs, negative prices)
- Cleans and validates ~50K transactions, filtering out ~10% of bad records
- Outputs a star schema (`fact_sales`, `dim_customer`, `dim_product`) compatible with Redshift/PostgreSQL
- Includes SQL scripts for the warehouse and a Jupyter notebook with exploratory analysis

## Architecture

```
Raw CSVs (data/raw/)  →  PySpark Transform  →  Clean CSVs (data/processed/)  →  Redshift/PostgreSQL
```

## Quick start

```bash
git clone https://github.com/your-user/etl-datalake-demo.git
cd etl-datalake-demo

python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# Generate sample data with quality issues
python scripts/generate_sample_data.py

# Run the ETL pipeline
python src/transform/transform_sales_data.py
```

Output goes to `data/processed/`. Check out the notebook in `notebooks/` for visualizations.

## Requirements

- Python 3.12
- Java 11+ (for PySpark)

## Project structure

```
data/
  raw/           # Input CSVs with intentional issues
  processed/     # Cleaned star schema output
notebooks/       # Exploratory analysis
scripts/         # Data generation
sql/             # DDL and analytical queries (Redshift-optimized)
src/transform/   # PySpark ETL code
```

## AWS deployment

The pipeline is designed to run on AWS Glue/EMR with minimal changes. Upload raw data to S3, run the transform job, and load results into Redshift using COPY commands.

## License

MIT
