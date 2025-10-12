"""
Data Generator for ETL Demo Project
Generates realistic sample data for customers, products, and sales.
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import os

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)
fake = Faker(['es_ES', 'es_MX', 'en_US'])
Faker.seed(42)

# Configuration
NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 150
NUM_SALES = 50000
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2024, 10, 11)

# Output paths
OUTPUT_DIR = 'data/raw'
os.makedirs(OUTPUT_DIR, exist_ok=True)


def generate_customers(n=NUM_CUSTOMERS):
    """Generate customer data with realistic information and data quality issues."""
    print(f"Generating {n} customers...")

    countries = ['Mexico', 'Spain', 'Argentina', 'Colombia', 'Chile', 'Peru', 'USA', 'Brazil']
    segments = ['Premium', 'Standard', 'Basic']
    segment_weights = [0.2, 0.5, 0.3]  # 20% Premium, 50% Standard, 30% Basic

    customers = []
    for i in range(1, n + 1):
        reg_date = fake.date_between(start_date=START_DATE, end_date=END_DATE)

        # Introduce data quality issues
        # 3% missing emails
        email = fake.email() if np.random.random() > 0.03 else None

        # 2% missing phone numbers
        phone = fake.phone_number() if np.random.random() > 0.02 else None

        # 1% missing city
        city = fake.city() if np.random.random() > 0.01 else None

        # 0.5% invalid email formats (for testing validation)
        if email and np.random.random() < 0.005:
            email = fake.user_name() + "@invalid"  # Missing domain

        # 1% duplicate emails (realistic issue)
        if i > 10 and np.random.random() < 0.01:
            email = customers[np.random.randint(0, len(customers))]['email']

        customers.append({
            'customer_id': i,
            'name': fake.name(),
            'email': email,
            'country': np.random.choice(countries),
            'registration_date': reg_date,
            'segment': np.random.choice(segments, p=segment_weights),
            'phone': phone,
            'city': city
        })

    df = pd.DataFrame(customers)
    return df


def generate_products(n=NUM_PRODUCTS):
    """Generate product catalog with various categories and data quality issues."""
    print(f"Generating {n} products...")

    categories = {
        'Electronics': ['Laptop', 'Smartphone', 'Tablet', 'Headphones', 'Smartwatch', 'Camera', 'Speaker', 'Monitor'],
        'Clothing': ['Jacket', 'Jeans', 'T-Shirt', 'Sneakers', 'Dress', 'Sweater', 'Shorts', 'Boots'],
        'Home': ['Lamp', 'Chair', 'Desk', 'Sofa', 'Table', 'Rug', 'Curtains', 'Bookshelf'],
        'Sports': ['Running Shoes', 'Yoga Mat', 'Dumbbell', 'Bicycle', 'Tennis Racket', 'Soccer Ball'],
        'Books': ['Fiction Novel', 'Programming Book', 'Cookbook', 'Biography', 'Self-Help'],
        'Beauty': ['Perfume', 'Skincare Set', 'Makeup Kit', 'Hair Dryer', 'Electric Razor']
    }

    brands = ['Samsung', 'Apple', 'Sony', 'Nike', 'Adidas', 'Dell', 'HP', 'Canon',
              'LG', 'Microsoft', 'Bose', 'JBL', 'Puma', 'Reebok', 'Logitech']

    products = []
    product_id = 101

    for _ in range(n):
        category = random.choice(list(categories.keys()))
        product_type = random.choice(categories[category])
        brand = random.choice(brands)

        # Price varies by category
        if category == 'Electronics':
            base_price = np.random.uniform(200, 2000)
        elif category == 'Clothing':
            base_price = np.random.uniform(30, 300)
        elif category == 'Home':
            base_price = np.random.uniform(50, 800)
        elif category == 'Sports':
            base_price = np.random.uniform(25, 500)
        elif category == 'Books':
            base_price = np.random.uniform(10, 50)
        else:  # Beauty
            base_price = np.random.uniform(20, 200)

        # Data quality issues
        # 1% missing supplier
        supplier = brand if np.random.random() > 0.01 else None

        # 2% negative or zero stock (inventory issues)
        if np.random.random() < 0.02:
            stock = np.random.randint(-10, 0)
        else:
            stock = np.random.randint(10, 500)

        # 0.5% negative prices (data entry errors)
        if np.random.random() < 0.005:
            price = -round(base_price, 2)
        else:
            price = round(base_price, 2)

        # 1% missing weight
        weight = round(np.random.uniform(0.1, 5.0), 2) if np.random.random() > 0.01 else None

        # 0.5% invalid ratings (outside 0-5 range)
        if np.random.random() < 0.005:
            rating = round(np.random.uniform(5.5, 10.0), 1)
        else:
            rating = round(np.random.uniform(3.5, 5.0), 1)

        products.append({
            'product_id': product_id,
            'product_name': f"{brand} {product_type}",
            'category': category,
            'unit_price': price,
            'supplier': supplier,
            'stock_quantity': stock,
            'weight_kg': weight,
            'rating': rating
        })
        product_id += 1

    df = pd.DataFrame(products)
    return df


def generate_sales(n=NUM_SALES, customers_df=None, products_df=None):
    """Generate sales transactions with realistic patterns and data quality issues."""
    print(f"Generating {n} sales transactions...")

    if customers_df is None or products_df is None:
        raise ValueError("Customer and product dataframes are required")

    customer_ids = customers_df['customer_id'].tolist()
    product_ids = products_df['product_id'].tolist()

    # Premium customers buy more frequently
    customer_segments = customers_df.set_index('customer_id')['segment'].to_dict()

    sales = []
    for sale_id in range(1001, 1001 + n):
        # Select customer with bias towards premium customers
        customer_id = random.choice(customer_ids)
        segment = customer_segments[customer_id]

        # Premium customers buy more items
        if segment == 'Premium':
            quantity = np.random.choice([1, 2, 3, 4, 5], p=[0.3, 0.3, 0.2, 0.1, 0.1])
        elif segment == 'Standard':
            quantity = np.random.choice([1, 2, 3], p=[0.5, 0.3, 0.2])
        else:  # Basic
            quantity = np.random.choice([1, 2], p=[0.7, 0.3])

        # Generate random date with some seasonal patterns
        date = fake.date_between(start_date=START_DATE, end_date=END_DATE)

        # Higher discounts during certain months (Black Friday, holidays)
        month = date.month
        if month in [11, 12]:  # Holiday season
            discount = np.random.choice([0, 5, 10, 15, 20], p=[0.3, 0.2, 0.2, 0.2, 0.1])
        else:
            discount = np.random.choice([0, 5, 10], p=[0.6, 0.3, 0.1])

        # Data quality issues in sales
        # 0.5% orphaned customer_ids (referential integrity issue)
        if np.random.random() < 0.005:
            customer_id = 9999  # Non-existent customer

        # 0.5% orphaned product_ids (referential integrity issue)
        product_id = random.choice(product_ids)
        if np.random.random() < 0.005:
            product_id = 9999  # Non-existent product

        # 1% negative quantities (data entry errors)
        if np.random.random() < 0.01:
            quantity = -quantity

        # 0.5% discounts > 100% (logical errors)
        if np.random.random() < 0.005:
            discount = np.random.randint(101, 150)

        # 2% missing dates
        if np.random.random() < 0.02:
            date = None

        # 1% future dates (logical errors)
        if date and np.random.random() < 0.01:
            date = fake.date_between(start_date=END_DATE, end_date=datetime(2026, 12, 31))

        # 0.5% duplicate sale_ids
        if sale_id > 2000 and np.random.random() < 0.005:
            sale_id = sales[np.random.randint(0, len(sales))]['sale_id']

        sales.append({
            'sale_id': sale_id,
            'customer_id': customer_id,
            'product_id': product_id,
            'date': date,
            'quantity': quantity,
            'discount_percent': discount,
            'payment_method': np.random.choice(['Credit Card', 'Debit Card', 'PayPal', 'Cash'],
                                              p=[0.5, 0.25, 0.15, 0.1]),
            'status': np.random.choice(['Completed', 'Shipped', 'Processing', 'Cancelled'],
                                      p=[0.85, 0.10, 0.03, 0.02])
        })

    df = pd.DataFrame(sales)
    df = df.sort_values('date').reset_index(drop=True)
    return df


def main():
    """Main function to generate all datasets."""
    print("=" * 60)
    print("ETL Demo - Sample Data Generator")
    print("=" * 60)

    # Generate data
    customers_df = generate_customers()
    products_df = generate_products()
    sales_df = generate_sales(customers_df=customers_df, products_df=products_df)

    # Save to CSV
    print("\nSaving data to CSV files...")
    customers_df.to_csv(f'{OUTPUT_DIR}/customers.csv', index=False)
    print(f"✓ Saved {len(customers_df)} customers to {OUTPUT_DIR}/customers.csv")

    products_df.to_csv(f'{OUTPUT_DIR}/products.csv', index=False)
    print(f"✓ Saved {len(products_df)} products to {OUTPUT_DIR}/products.csv")

    sales_df.to_csv(f'{OUTPUT_DIR}/sales.csv', index=False)
    print(f"✓ Saved {len(sales_df)} sales to {OUTPUT_DIR}/sales.csv")

    # Print summary statistics
    print("\n" + "=" * 60)
    print("Data Generation Summary")
    print("=" * 60)
    print(f"Total Customers: {len(customers_df):,}")
    print(f"Total Products: {len(products_df):,}")
    print(f"Total Sales: {len(sales_df):,}")

    # Handle date range with nulls
    valid_dates = sales_df['date'].dropna()
    if len(valid_dates) > 0:
        print(f"\nDate Range: {valid_dates.min()} to {valid_dates.max()}")

    print(f"\nProduct Categories: {products_df['category'].nunique()}")
    print(products_df['category'].value_counts().to_string())
    print(f"\nCustomer Segments:")
    print(customers_df['segment'].value_counts().to_string())
    print(f"\nTop Countries:")
    print(customers_df['country'].value_counts().head(5).to_string())

    # Data quality summary
    print("\n" + "=" * 60)
    print("Data Quality Issues Summary (intentional)")
    print("=" * 60)
    print(f"Customers with missing emails: {customers_df['email'].isna().sum()}")
    print(f"Customers with missing phones: {customers_df['phone'].isna().sum()}")
    print(f"Products with negative prices: {(products_df['unit_price'] < 0).sum()}")
    print(f"Products with negative stock: {(products_df['stock_quantity'] < 0).sum()}")
    print(f"Sales with missing dates: {sales_df['date'].isna().sum()}")
    print(f"Sales with negative quantities: {(sales_df['quantity'] < 0).sum()}")

    print("\n" + "=" * 60)
    print("Data generation completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()