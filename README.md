# ETL Data Lake Demo · End‑to‑End Data Engineering Project

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-ETL-orange?logo=apache-spark)
![AWS Ready](https://img.shields.io/badge/AWS-Ready-FF9900?logo=amazon-aws)
![SQL Analytics](https://img.shields.io/badge/SQL-Analytics-4169E1?logo=postgresql)

**[English](#english)** | **[Español](#español)**

---

## English

**Data Lake → Data Warehouse** pipeline that cleanses 50K+ transactions with PySpark, builds a **star schema ready for Redshift**, and delivers analytical assets for dashboards. The goal is to demonstrate technical mastery, data quality management, and cloud-ready solutions mindset.

> Designed as a professional portfolio project: reproducible, documented, and with tangible results.

### Highlights

- 3 synthetic datasets (customers, products, sales) with realistic quality issues
- PySpark pipeline that cleanses **9.6% of invalid records** and generates `fact_sales`, `dim_customer`, `dim_product`
- Production-grade SQL: DDL optimized for Redshift + aggregated views and business-ready queries for BI
- EDA notebook with business storytelling and commercial performance visualizations
- Ready to migrate to AWS (S3, Glue, Redshift, QuickSight) with no major rewrite

### Architecture at a Glance

```
┌──────────────┐   ┌────────────────────────┐   ┌─────────────────────┐
│  Data Lake   │   │  PySpark Transform     │   │  Data Warehouse     │
│  (S3/local)  │──►│  Validation & Star     │──►│  Amazon Redshift/   │
│ customers.csv│   │  Schema clean_*        │   │  PostgreSQL         │
│ products.csv │   │  functions, business   │   │                     │
│ sales.csv    │   │  metrics, saved to     │   │ fact_sales + dims   │
│              │   │  processed/            │   │                     │
└──────────────┘   └────────────────────────┘   └─────────────────────┘
```

### Pipeline Journey

1. **Data Generation** – `scripts/generate_sample_data.py` creates datasets with fixed seeds and intentional issues (nulls, duplicates, orphan FKs, negative prices).
2. **PySpark Transformation** – `src/transform/transform_sales_data.py` cleanses, validates, and builds the star schema with calculated metrics (`total_amount`, `discount_amount`, etc.).
3. **Persistence** – Outputs in `data/processed/` (partitioned CSV) ready for `COPY` commands to Redshift.
4. **Analytical Validation** – `notebooks/exploratory_analysis.ipynb` and `sql/analytical_queries.sql` test performance, trends, and key KPIs.

### Data Assets

#### Resulting Star Schema

| Table          | Brief Description                                   |
|----------------|-----------------------------------------------------|
| `fact_sales`   | Transactions with derived metrics and status        |
| `dim_customer` | Cleansed customers with segments and geography      |
| `dim_product`  | Catalog with prices, ratings, and categories        |

See full definition in `sql/create_tables.sql` and DBML model in `sql/dbdiagram_schema.txt`.

#### Quality Metrics

| Dataset   | Raw Records | Clean Records | Issues Resolved |
|-----------|-------------|---------------|-----------------|
| Customers | 1,000       | 963           | Invalid/duplicate emails, nulls, type casting |
| Products  | 150         | 148           | Negative prices, invalid stock, out-of-range ratings |
| Sales     | 50,000      | 45,179        | Future/null dates, orphan FKs, negative quantities, duplicates |

The pipeline includes detailed logs (`utils/logging_config.py`) for traceability of each step.

### Analytical Toolkit

- `sql/create_tables.sql` – DDL with DISTKEY/SORTKEY, aggregated views, and validations.
- `sql/analytical_queries.sql` – Revenue by category, LTV by segment, cohorts, discounts, monthly trends.
- `notebooks/exploratory_analysis.ipynb` – Visualizations (top products, seasonality, discount impact) and business findings.

### Requirements

- Python 3.12
- Java/JDK 11+ (required for PySpark)
- Dependencies from `requirements.txt`
- (Optional) Redshift/PostgreSQL cluster to run SQL scripts

### Quickstart

```bash
# 1. Clone the repository
git clone https://github.com/your-user/etl-datalake-demo.git
cd etl-datalake-demo

# 2. Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Generate/refresh raw data
python scripts/generate_sample_data.py

# 5. Run PySpark pipeline
python src/transform/transform_sales_data.py

# 6. (Optional) Explore results with EDA notebook
jupyter notebook notebooks/exploratory_analysis.ipynb
```

Processed CSVs will be in `data/processed/`. To load them into Redshift, use COPY from S3 or integrate with AWS Glue.

### AWS-Ready

- **Storage**: Upload `data/raw/` to an S3 bucket (raw zone).
- **Transform**: Execute `transform_sales_data.py` as a PySpark job in AWS Glue/EMR.
- **Load**: Use `sql/create_tables.sql` in Redshift and consume processed CSVs stored in S3.
- **Visualization**: Connect QuickSight to Redshift and leverage aggregated views.

### Project Structure

```
etl-datalake-demo/
├── data/
│   ├── raw/          # Customers/products/sales with controlled issues
│   ├── processed/    # Clean star schema (PySpark outputs)
│   └── analytics/    # Space for aggregates and dashboards
├── notebooks/
│   └── exploratory_analysis.ipynb
├── scripts/
│   └── generate_sample_data.py
├── sql/
│   ├── create_tables.sql
│   ├── analytical_queries.sql
│   └── dbdiagram_schema.txt
├── src/
│   ├── transform/transform_sales_data.py
│   └── utils/logging_config.py
└── README.md
```

### Portfolio Story

- **Problem**: Messy e-commerce data preventing reliable analysis.
- **Solution**: Reproducible PySpark pipeline that validates and structures information into a star schema.
- **Impact**: 90% of sales recovered, critical KPIs calculated, and dashboards enabled in minutes.
- **Scalability**: Architecture ready to migrate to AWS serverless services with no drastic changes.

### Roadmap

- [ ] Publish datasets to S3 and orchestrate with EventBridge/Step Functions
- [ ] Migrate transformations to AWS Glue for serverless execution
- [ ] Automate loads to Amazon Redshift (COPY/Glue Job)
- [ ] Build QuickSight dashboard with generated views
- [ ] Add quality monitoring with Great Expectations or Deequ

### Technologies

- **Python 3.12**, **PySpark** for distributed ETL.
- **Pandas, Matplotlib, Seaborn** for analysis and visualization.
- **PostgreSQL / Amazon Redshift** as data warehouse.
- **Faker** to generate data with realistic variations.
- **SQL** for dimensional modeling and business queries.

### Author

- Maintainer: `@raulps819`

### License

This project is distributed under the MIT License. See `LICENSE` for details.

---

## Español

Pipeline de datos tipo **Data Lake → Data Warehouse** que limpia 50K+ transacciones con PySpark, levanta un **esquema estrella listo para Redshift** y entrega material analítico para dashboards. El objetivo es mostrar dominio técnico, manejo de calidad de datos y mentalidad de soluciones en la nube.

> Diseñado como proyecto de portafolio profesional: reproducible, documentado y con resultados tangibles.

### Highlights

- 3 datasets sintéticos (clientes, productos, ventas) con problemas de calidad realistas
- Pipeline PySpark que sanea el **9.6% de registros inválidos** y genera `fact_sales`, `dim_customer`, `dim_product`
- SQL productivo: DDL optimizado para Redshift + vistas y queries de negocio listas para BI
- Notebook EDA con storytelling de negocio y visualizaciones de performance comercial
- Preparado para migrar a AWS (S3, Glue, Redshift, QuickSight) sin reescritura masiva

### Arquitectura a Simple Vista

```
┌──────────────┐   ┌────────────────────────┐   ┌─────────────────────┐
│  Data Lake   │   │  Transformación        │   │  Data Warehouse     │
│  (S3/local)  │──►│  PySpark Validación y  │──►│  Amazon Redshift /  │
│ customers.csv│   │  Star Schema clean_*   │   │  PostgreSQL         │
│ products.csv │   │  funciones, métricas   │   │                     │
│ sales.csv    │   │  de negocio, guardado  │   │ fact_sales + dims   │
│              │   │  en processed/         │   │                     │
└──────────────┘   └────────────────────────┘   └─────────────────────┘
```

### Recorrido del Pipeline

1. **Generación de datos** – `scripts/generate_sample_data.py` crea datasets con seeds fijos y issues intencionales (nulos, duplicados, FKs huérfanas, precios negativos).
2. **Transformación PySpark** – `src/transform/transform_sales_data.py` limpia, valida y arma el star schema con métricas calculadas (`total_amount`, `discount_amount`, etc.).
3. **Persistencia** – Salidas en `data/processed/` (CSV particionados) listas para comandos `COPY` hacia Redshift.
4. **Validación Analítica** – `notebooks/exploratory_analysis.ipynb` y `sql/analytical_queries.sql` prueban rendimiento, tendencias y KPIs clave.

### Activos de Datos

#### Star Schema Resultante

| Tabla          | Descripción breve                                  |
|----------------|----------------------------------------------------|
| `fact_sales`   | Transacciones con métricas derivadas y estado      |
| `dim_customer` | Clientes depurados con segmentos y geografía       |
| `dim_product`  | Catálogo con precios, ratings y categorías         |

Consulta la definición completa en `sql/create_tables.sql` y el modelo DBML en `sql/dbdiagram_schema.txt`.

#### Métricas de Calidad

| Dataset   | Registros crudos | Registros limpios | Issues resueltos |
|-----------|------------------|-------------------|------------------|
| Customers | 1,000            | 963               | Emails inválidos/duplicados, nulos, tipificación |
| Products  | 150              | 148               | Precios negativos, stock inválido, ratings fuera de rango |
| Sales     | 50,000           | 45,179            | Fechas futuras/nulas, FKs huérfanas, cantidades negativas, duplicados |

El pipeline incluye logs detallados (`utils/logging_config.py`) para trazabilidad de cada paso.

### Toolkit Analítico

- `sql/create_tables.sql` – DDL con DISTKEY/SORTKEY, vistas agregadas y validaciones.
- `sql/analytical_queries.sql` – Revenue por categoría, LTV por segmento, cohortes, descuentos, tendencias mensuales.
- `notebooks/exploratory_analysis.ipynb` – Visualizaciones (top productos, estacionalidad, impacto de descuentos) y hallazgos de negocio.

### Requisitos

- Python 3.12
- Java/JDK 11+ (necesario para PySpark)
- Dependencias de `requirements.txt`
- (Opcional) Cluster Redshift/PostgreSQL para ejecutar los scripts SQL

### Quickstart

```bash
# 1. Clona el repositorio
git clone https://github.com/tu-usuario/etl-datalake-demo.git
cd etl-datalake-demo

# 2. Crea y activa un entorno virtual
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate

# 3. Instala dependencias
pip install -r requirements.txt

# 4. Genera/actualiza los datos crudos
python scripts/generate_sample_data.py

# 5. Ejecuta el pipeline PySpark
python src/transform/transform_sales_data.py

# 6. (Opcional) Explora resultados con el notebook EDA
jupyter notebook notebooks/exploratory_analysis.ipynb
```

Los CSV procesados quedarán en `data/processed/`. Para cargarlos en Redshift, usa COPY desde S3 o integra con AWS Glue.

### Preparado para AWS

- **Storage**: Subir `data/raw/` a un bucket S3 (raw zone).
- **Transform**: Ejecutar `transform_sales_data.py` como job PySpark en AWS Glue/EMR.
- **Load**: Usar `sql/create_tables.sql` en Redshift y consumir los CSV procesados almacenados en S3.
- **Visualización**: Conectar QuickSight a Redshift y aprovechar las vistas agregadas.

### Estructura del Proyecto

```
etl-datalake-demo/
├── data/
│   ├── raw/          # Customers/products/sales con issues controlados
│   ├── processed/    # Star schema limpio (PySpark outputs)
│   └── analytics/    # Espacio para agregados y dashboards
├── notebooks/
│   └── exploratory_analysis.ipynb
├── scripts/
│   └── generate_sample_data.py
├── sql/
│   ├── create_tables.sql
│   ├── analytical_queries.sql
│   └── dbdiagram_schema.txt
├── src/
│   ├── transform/transform_sales_data.py
│   └── utils/logging_config.py
└── README.md
```

### Historia para Portafolio

- **Problema**: Datos e-commerce desordenados que impiden análisis confiables.
- **Solución**: Pipeline reproducible con PySpark que valida y estructura la información en un modelo estrella.
- **Impacto**: Se recupera el 90% de las ventas, se calculan KPIs críticos y se habilitan dashboards en minutos.
- **Escalabilidad**: Arquitectura lista para migrar a servicios serverless de AWS sin cambios drásticos.

### Roadmap

- [ ] Publicar datasets en S3 y orquestar con EventBridge/Step Functions
- [ ] Migrar transformaciones a AWS Glue para ejecución serverless
- [ ] Automatizar cargas a Amazon Redshift (COPY/Glue Job)
- [ ] Construir dashboard QuickSight con vistas generadas
- [ ] Añadir monitoreo de calidad con Great Expectations o Deequ

### Tecnologías

- **Python 3.12**, **PySpark** para ETL distribuidos.
- **Pandas, Matplotlib, Seaborn** para análisis y visualización.
- **PostgreSQL / Amazon Redshift** como data warehouse.
- **Faker** para generar datos con variaciones realistas.
- **SQL** para modelado dimensional y consultas de negocio.

### Autor

- Maintainer: `@raulps819`

### Licencia

Este proyecto se distribuye bajo la licencia MIT. Revisa `LICENSE` para más detalles.
