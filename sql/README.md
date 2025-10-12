# SQL Scripts

SQL scripts for implementing the data warehouse star schema and analytical queries.

## Files

### create_tables.sql
Defines the complete star schema optimized for Amazon Redshift.

**Tables:**
- **customers** - Customer dimension with demographics and segmentation
- **products** - Product catalog dimension with pricing and ratings
- **sales** - Sales fact table with transaction metrics

**Redshift Optimizations:**
- Distribution keys (DISTKEY) for co-located joins
- Sort keys (SORTKEY) for efficient range scans
- Referential integrity constraints
- Check constraints for data validation

**Pre-built Views:**
- `vw_sales_by_category` - Revenue aggregated by product category
- `vw_sales_by_segment` - Customer segment performance
- `vw_monthly_sales` - Time series sales trends

### analytical_queries.sql
Production-ready business intelligence queries demonstrating:

**Sales Analytics:**
- Top products by revenue and units sold
- Category performance with monthly trends
- Transaction patterns and average order values

**Customer Analytics:**
- Customer lifetime value (LTV) by segment
- Top customers by total spend
- Purchase frequency analysis

**Discount Analysis:**
- Discount effectiveness by category
- Promotional campaign ROI
- Margin impact calculations

**Time-Based Analytics:**
- Year-over-year growth rates
- Day-of-week sales patterns
- Seasonal trend identification

**Cohort Analysis:**
- Customer retention by registration cohort
- Revenue progression over customer lifecycle

**Product Performance:**
- Product scorecards with key metrics
- Cross-sell opportunities
- Inventory optimization insights

## Schema Design

Star schema architecture provides:
- **Simple joins** - Easy to understand and query
- **Query performance** - Optimized for analytical workloads
- **Aggregation speed** - Pre-calculated metrics in fact table
- **Scalability** - Distributes well across Redshift nodes

## Usage

**Deploy to Redshift:**
```bash
psql -h <cluster-endpoint> -U <username> -d <database> -f create_tables.sql
```

**Run analytics:**
```bash
psql -h <cluster-endpoint> -U <username> -d <database> -f analytical_queries.sql
```

**Or use with any PostgreSQL-compatible database:**
```bash
psql -U postgres -d analytics -f create_tables.sql
```

## Distribution Strategy

- **customers table**: `DISTKEY(customer_id)` - distributes by customer for efficient joins with sales
- **products table**: `DISTSTYLE ALL` - small dimension replicated to all nodes
- **sales table**: `DISTKEY(customer_id)` - co-located with customers for join optimization

## Sort Keys

- **customers**: `(customer_id, registration_date)` - supports customer lookups and time-based queries
- **products**: `(category, product_id)` - optimizes category filtering
- **sales**: `(sale_date, customer_id)` - time-series queries and customer analysis
