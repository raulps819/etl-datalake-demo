# ETL Data Lake Demo

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-ETL-orange?logo=apache-spark)
![AWS](https://img.shields.io/badge/AWS-Ready-FF9900?logo=amazon-aws)
![SQL](https://img.shields.io/badge/SQL-Analytics-4169E1?logo=postgresql)

Pipeline de datos end-to-end que simula un flujo **Data Lake -> Data Warehouse** usando PySpark y un modelo analitico tipo estrella. El objetivo es demostrar dominio en ingenieria de datos, limpieza de datasets con problemas reales y preparacion de informacion para cargas en Amazon Redshift.

## Resumen del Proyecto
- 50K+ transacciones de ventas generadas sinteticamente con inconsistencias intencionales.
- Limpieza de datos con PySpark para dejar un esquema estrella consistente.
- Scripts SQL listos para desplegar el modelo analitico en Redshift/PostgreSQL.
- Notebook de analisis exploratorio para validar la calidad del pipeline y hallar insights.

## Arquitectura del Flujo
```
Data Lake (S3)          Transformacion (PySpark)        Data Warehouse (Redshift)
---------------  ->  ---------------------------  ->  --------------------------
customers.csv          - Limpieza de datos               dim_customer
products.csv           - Validaciones                    dim_product
sales.csv              - Creacion de star schema         fact_sales
```

1. `scripts/generate_sample_data.py` crea datasets crudos con problemas de calidad controlados.  
2. `src/transform/transform_sales_data.py` limpia y consolida la informacion con PySpark.  
3. Los datasets procesados se almacenan en `data/processed/` listos para cargas en Redshift.  
4. Las consultas analiticas viven en `sql/` y se pueden ejecutar sobre el warehouse.

## Caracteristicas Clave
- ETL PySpark con reglas de negocio y validaciones robustas.
- Manejo del 9.6% de registros invalidos en los datos de ventas.
- Esquema estrella con metricas derivadas y dimensiones ricas.
- Queries analiticas preconstruidas para validar el modelo de negocio.
- Notebook de EDA para presentar hallazgos y visualizaciones clave.
- Preparado para migrarse a servicios gestionados de AWS (Glue, Redshift, QuickSight).

## Calidad de Datos
| Dataset  | Registros crudos | Registros limpios | Problemas resueltos |
|----------|------------------|-------------------|---------------------|
| Customers | 1,000 | 963 | Emails invalidos/duplicados, valores nulos, tipos inconsistentes |
| Products  | 150  | 148 | Precios negativos, stock invalido, ratings fuera de rango |
| Sales     | 50,000 | 45,179 | Fechas futuras, FKs huerfanas, cantidades negativas, duplicados |

**Tipos de issues tratados:** correccion de descuentos, normalizacion de fechas, integridad referencial, imputaciones controladas y deduplicacion.

## Estructura del Proyecto
```
etl-datalake-demo/
|-- data/
|   |-- raw/              # Datos crudos (customers, products, sales)
|   |-- processed/        # Salidas limpias: dim_customer, dim_product, fact_sales
|   `-- analytics/        # Sets agregados para reporting
|-- notebooks/
|   `-- exploratory_analysis.ipynb
|-- scripts/
|   `-- generate_sample_data.py
|-- src/
|   |-- transform/
|   |   `-- transform_sales_data.py
|   `-- utils/
|       `-- logging_config.py
|-- sql/
|   |-- create_tables.sql
|   `-- analytical_queries.sql
|-- requirements.txt
`-- README.md
```

## Requisitos Previos
- Python 3.12
- Java/JDK 11+ para ejecutar PySpark
- Dependencias listadas en `requirements.txt`
- (Opcional) Acceso a un cluster Redshift o PostgreSQL para ejecutar los scripts SQL

## Instalacion y Ejecucion
```bash
# 1. Clonar el repositorio
git clone https://github.com/tu-usuario/etl-datalake-demo.git
cd etl-datalake-demo

# 2. Crear entorno virtual
python -m venv venv
source venv/bin/activate

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Generar o refrescar datos crudos
python scripts/generate_sample_data.py

# 5. Ejecutar el pipeline PySpark
python src/transform/transform_sales_data.py

# 6. (Opcional) Abrir el notebook de EDA
jupyter notebook notebooks/exploratory_analysis.ipynb
```

Los datasets procesados se guardan en `data/processed/` y pueden cargarse en Redshift mediante el comando `COPY` o integraciones de Glue.

## Resultados del Pipeline
- Dimensiones: `dim_customer`, `dim_product`
- Tabla de hechos: `fact_sales` con metricas derivadas (`total_amount`, `discount_amount`, etc.)
- Agregados para dashboards en `data/analytics/`
- Logs detallados durante la ejecucion para trazar calidad y volumen

## SQL y Analisis
- `sql/create_tables.sql`: DDL optimizado para Amazon Redshift (DISTKEY, SORTKEY, vistas materializadas).
- `sql/analytical_queries.sql`: Consultas para revenue por categoria, LTV por segmento, analisis de descuentos, cohortes y series temporales.

## Exploratory Data Analysis
`notebooks/exploratory_analysis.ipynb` documenta estadisticas descriptivas, visualizaciones y hallazgos de negocio:
- Distribucion de ventas por categoria y segmento.
- Top 10 productos por revenue.
- Ventas mensuales y estacionalidad.
- Impacto de descuentos y correlaciones clave.

## Roadmap AWS
- [ ] Cargar datos en Amazon S3 como data lake.
- [ ] Migrar la transformacion a AWS Glue (PySpark job).
- [ ] Automatizar cargas a Amazon Redshift.
- [ ] Publicar dashboards en Amazon QuickSight.
- [ ] Orquestar el pipeline con EventBridge / Step Functions.

## Tecnologias
- **Python 3.12**, **PySpark** para ETL distribuidos.
- **Pandas, Matplotlib, Seaborn** para analisis y visualizacion.
- **PostgreSQL / Amazon Redshift** como data warehouse.
- **Faker** para generar datos con variaciones realistas.
- **SQL** para modelado dimensional y consultas de negocio.

## Autor
- Maintainer: `@raulps819`

## Licencia
Este proyecto se distribuye bajo la licencia MIT. Revisa `LICENSE` para mas detalles.
