# Data Directory

This directory contains the datasets used throughout the ETL pipeline.

## Directory Structure

```
data/
├── raw/          # Source data (unprocessed)
├── processed/    # Cleaned and transformed data
└── analytics/    # Data ready for warehouse loading
```

## Data Generation

The sample data was generated using `scripts/generate_sample_data.py`, which creates realistic e-commerce data with intentional quality issues to simulate real-world scenarios.

### Datasets

**customers.csv** (1,000 records)
- Customer information including demographics and segmentation
- Fields: customer_id, name, email, country, registration_date, segment, phone, city

**products.csv** (150 records)
- Product catalog across multiple categories
- Fields: product_id, product_name, category, unit_price, supplier, stock_quantity, weight_kg, rating

**sales.csv** (50,000 records)
- Transaction records from 2023-01-01 to 2024-10-11
- Fields: sale_id, customer_id, product_id, date, quantity, discount_percent, payment_method, status

### Known Data Quality Issues

The raw data intentionally includes common data quality problems:

- Missing values (emails, phone numbers, dates)
- Invalid formats (email addresses, out-of-range ratings)
- Logical errors (negative quantities, negative prices, future dates)
- Referential integrity issues (orphaned foreign keys)
- Duplicate records

These issues are addressed in the transformation phase of the ETL pipeline.

## Regenerating Data

To regenerate the sample data:

```bash
python scripts/generate_sample_data.py
```

Note: Data generation uses a fixed random seed for reproducibility.
