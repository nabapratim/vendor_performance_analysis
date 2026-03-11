# Vendor Performance Analytics Pipeline

## Project Overview

This project builds an **end-to-end vendor performance analytics pipeline** using SQL, Python, and statistical analysis. The goal is to transform raw purchasing and sales data into actionable insights that help organizations optimize vendor relationships, improve profitability, and reduce inventory risk.

The project demonstrates a complete analytics workflow:

```text
Raw Data → Data Warehouse (Silver/Gold Layers) → Data Pipeline → Analysis → Insights
```

It combines **data engineering, analytics, and statistical validation** to answer key business questions about vendor performance.

---

# Business Problem

Organizations often manage hundreds of vendors but only a few contribute significantly to revenue and profitability. Poor vendor evaluation can lead to:

* low-margin vendor partnerships
* dead stock inventory
* inefficient procurement strategies
* supplier concentration risk

This project aims to answer:

* Which vendors generate the most revenue and profit?
* Which vendors create inventory risk?
* Which vendors should be promoted or renegotiated?
* Are vendor performance differences statistically significant?

---

# Project Architecture

```
Vendor-Analytics
│
├── 00_data
│   ├── raw
│   │   ├── purchases.csv
│   │   ├── purchase_prices.csv
│   │   ├── sales.csv
│   │   └── vendor_invoice.csv
│   └── outputs
│
├── 01_sql
│   ├── 01_silver_layer
│   │   ├── silver_ddl.sql
│   │   ├── silver_data_insert.sql
│   │   ├── indexing.sql
│   │   └── silver_refresh_stored_procedure.sql
│   │
│   ├── 02_gold_layer
│   │   ├── summary_table.sql
│   │   ├── gold_indexing.sql
│   │   └── gold_store_procedure.sql
│   │
│   └── 03_pipeline
│       └── pipeline_warehouse_refresh.sql
│
├── 02_src
│   ├── extraction.py
│   ├── ingestion.py
│   ├── db.py
│   └── main.py
│
├── 04_notebooks
│   └── performance_analysis.ipynb
│
├── config
│   └── db_config.env
│
└── reports
```

---

# Data Warehouse Design

The project follows a **layered data warehouse architecture**.

## Bronze Layer

Bronze layer represents the raw ingestion layer of the data warehouse.

Scripts:

```
db.py
indestion.py
extraction.py
```
Responsibilities:

* raw and uncleaned data
* no business transformation
* used in input for silver layer transformation

---

## Silver Layer

Cleaned and structured data created from raw source files.

Scripts:

```
silver_ddl.sql
silver_data_insert.sql
silver_refresh_stored_procedure.sql
indexing.sql
```

Responsibilities:

* data cleaning
* schema definition
* indexing for query optimization
* transformation from raw files to structured tables

---

## Gold Layer

Business-ready analytical tables.

Scripts:

```
summary_table.sql
gold_indexing.sql
gold_store_procedure.sql
```

This layer aggregates vendor-level metrics such as:

* total purchase quantity
* total sales quantity
* purchase cost
* sales revenue
* profit margin
* freight cost
* sell-through rate

---

# Data Pipeline

Warehouse refresh is automated using SQL pipeline scripts.

```
pipeline_warehouse_refresh.sql
```

Pipeline flow:

```
Raw CSV Files
      ↓
Silver Layer (cleaned tables)
      ↓
Gold Layer (aggregated analytics tables)
      ↓
Vendor Performance Analysis
```

---

# Python Data Pipeline

Python scripts manage extraction, ingestion, and orchestration.

## Components

**extraction.py**

Extracts raw CSV data.

**ingestion.py**

Loads processed data into PostgreSQL.

**db.py**

Manages database connections.

**main.py**

Runs the entire pipeline workflow.

---

# Exploratory Data Analysis

Analysis was conducted in:

```
04_notebooks/performance_analysis.ipynb
```

Key metrics analyzed:

* revenue contribution
* profit margins
* sell-through rate
* vendor concentration
* dead stock risk
* procurement efficiency

---

# Vendor Performance Framework

## Vendor Risk Matrix

Vendors are segmented based on **sales volume and profitability**.

| Category                | Meaning                     | Recommended Action      |
| ----------------------- | --------------------------- | ----------------------- |
| Strategic Vendors       | High sales and high margins | Strengthen partnerships |
| Margin Risk             | High sales but low margins  | Renegotiate pricing     |
| Growth Opportunity      | Low sales but high margins  | Increase promotion      |
| Underperforming Vendors | Low sales and low margins   | Reduce or eliminate     |

An interactive **Vendor Risk Matrix** was created using Plotly.

---

# Inventory Risk Analysis

Dead stock was calculated to identify vendors causing unsold inventory.

```
dead_stock_qty = purchase_qty − sales_qty
dead_stock_ratio = dead_stock_qty / purchase_qty
```

This highlights vendors contributing to inventory buildup.

---

# Procurement Efficiency Analysis

To evaluate procurement efficiency, unit cost was analyzed.

```
unit_price = purchase_amount / purchase_qty
```

Orders were categorized into:

* Small
* Medium
* Large

This verifies whether larger purchase volumes reduce cost per unit.

---

# Statistical Validation

## ANOVA Test

Tested whether profit margins differ across vendor groups.

```
F-statistic = 49.65
p-value = 1.16e-20
```

Conclusion:

Vendor groups have **significantly different profit margins**.

---

## Welch Two-Sample T-Test

Compared profit margins between **Strategic Vendors** and **Underperforming Vendors**.

```
t-statistic = 10.42
p-value = 9.82e-15
```

Conclusion:

Strategic vendors generate **significantly higher profit margins**.

---

## Correlation Analysis

Tested relationship between sales volume and profit margin.

```
Correlation = -0.017
p-value = 0.855
```

Conclusion:

Sales volume does **not significantly influence profit margins**.

---

# Key Insights

* Vendor segmentation reveals clear performance differences.
* Strategic vendors generate significantly higher profitability.
* Some vendors produce low revenue and poor margins simultaneously.
* Inventory risk arises from vendors with low sell-through rates.
* Sales volume alone does not explain profitability.

---

# Business Recommendations

* Prioritize partnerships with **Strategic Vendors**.
* Renegotiate contracts with **Margin Risk Vendors**.
* Promote **Growth Opportunity Vendors** to increase revenue.
* Reevaluate or discontinue **Underperforming Vendors**.
* Monitor inventory risk from low sell-through vendors.

---

# Key Skills Demonstrated

This project highlights multiple analytics competencies:

* Data warehouse design (Silver & Gold layers)
* SQL performance optimization and indexing
* ETL pipeline development
* Data transformation and aggregation
* Exploratory data analysis
* Statistical hypothesis testing
* Interactive visualization
* Business insight generation

---

# Technology Stack

* Python
* PostgreSQL
* SQL
* Pandas
* NumPy
* Plotly
* Seaborn
* SciPy
* Jupyter Notebook

---

# Impact of the Analysis

The framework helps businesses:

* identify high-value vendors
* reduce inventory risk
* optimize procurement decisions
* detect underperforming suppliers
* improve vendor portfolio management

---

# Future Improvements

Potential enhancements:

* vendor performance dashboard
* automated pipeline scheduling
* machine learning models for margin prediction
* real-time vendor monitoring

---

## Contact

Author: Nava Pratim Dutta

LinkedIn: ([add your LinkedIn profile link](https://www.linkedin.com/in/nabapratimdutta/))

Email:[email] (nabapratim.dutta@gmail.com)

GitHub:([your GitHub profile link](https://github.com/nabapratim/vendor_performance_analysis))

Feel free to connect if you are interested in data analytics, supply chain analytics, or collaborative projects.