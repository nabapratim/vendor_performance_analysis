/*========================================================================================
 GOLD LAYER QUERY: Vendor Performance & Price Analysis
------------------------------------------------------------------------------------------
Purpose:
Create a business-ready dataset to analyze vendor performance, pricing efficiency,
and purchasing strategy.

Key metrics produced:
- avg_purchase_price (weighted)
- actual_price (latest vendor price)
- total purchase & sales quantities
- allocated freight cost
- revenue vs cost analysis

Grain of dataset:
vendor_number + brand + description

Reason:
Allows detailed product-level vendor performance analysis while avoiding duplicate
freight costs.
========================================================================================*/


/*-----------------------------------------------------------------------------
STEP 1 — Aggregate purchase transactions
-------------------------------------------------------------------------------
Why:
Purchases table contains transaction-level records.
We aggregate them to vendor + brand + product level to reduce dataset size.

Important:
Weighted average purchase price is used instead of AVG().

Reason:
AVG(price) ignores quantity differences and produces misleading costs.
Weighted price = total spend / total quantity.
-----------------------------------------------------------------------------*/
/*===========================================================================
GOLD LAYER: vendor_performance (Materialized View)
===========================================================================*/
DROP MATERIALIZED VIEW gold.vendor_performance;

CREATE MATERIALIZED VIEW gold.vendor_performance AS

WITH purchase_agg AS (

SELECT 
    p.vendor_number,
    p.vendor_name,
    p.brand,
    p.description,

    -- total quantity purchased
    SUM(p.quantity) AS total_purchase_qty,

    -- total money spent purchasing
    SUM(p.dollars) AS total_purchase_amount,

    -- weighted purchase price per unit
    SUM(p.dollars) / NULLIF(SUM(p.quantity),0) AS avg_purchase_price

FROM silver.purchases p

GROUP BY 
    p.vendor_number,
    p.vendor_name,
    p.brand,
    p.description
),


/*-----------------------------------------------------------------------------
STEP 2 — Extract latest vendor price
-------------------------------------------------------------------------------
Why:
purchase_prices table may contain historical price updates.

Using AVG(price) would produce a value that never actually existed.

DISTINCT ON with updated_at ensures we take the latest vendor price.
-----------------------------------------------------------------------------*/

price_summary AS (

SELECT DISTINCT ON (vendor_number, brand)

    vendor_number,
    brand,
    price AS actual_price

FROM silver.purchase_prices
),


/*-----------------------------------------------------------------------------
STEP 3 — Aggregate sales data
-------------------------------------------------------------------------------
Why:
Sales table is transactional.
Aggregating reduces row volume before joining with other datasets.
-----------------------------------------------------------------------------*/

sales_summary AS (

SELECT 
    vendor_no AS vendor_number,
    brand,

    SUM(sales_dollars) AS total_sales_amount,
    SUM(sales_quantity) AS total_sales_qty,
    SUM(excise_tax) AS excise_tax

FROM silver.sales

GROUP BY vendor_no, brand
),


/*-----------------------------------------------------------------------------
STEP 4 — Aggregate freight costs
-------------------------------------------------------------------------------
Why:
Freight costs exist only at vendor level in vendor_invoice table.
Later we distribute this cost across vendor products proportionally.
-----------------------------------------------------------------------------*/

freight_summary AS (

SELECT 
    vendor_number,
    SUM(freight) AS freight_cost

FROM silver.vendor_invoice

GROUP BY vendor_number
),


/*-----------------------------------------------------------------------------
STEP 5 — Combine purchase, price, and sales data
-------------------------------------------------------------------------------
LEFT JOIN is used to avoid dropping rows if one dataset is missing information.

Example:
Vendor has purchases but no sales yet → row should still exist.
-----------------------------------------------------------------------------*/

summary_stg AS (

SELECT
    p.vendor_number,
    p.vendor_name,
    p.brand,
    p.description,

    pr.actual_price,
    p.avg_purchase_price,

    p.total_purchase_qty,
    p.total_purchase_amount,

    s.total_sales_amount,
    s.total_sales_qty,
    s.excise_tax

FROM purchase_agg p

LEFT JOIN price_summary pr
    ON p.brand = pr.brand

LEFT JOIN sales_summary s
    ON p.vendor_number = s.vendor_number
    AND p.brand = s.brand
),


/*-----------------------------------------------------------------------------
STEP 6 — Allocate freight cost proportionally
-------------------------------------------------------------------------------
Problem:
Freight is recorded at vendor level only.

If joined directly, freight would repeat for every product row,
inflating total freight values.

Solution:
Allocate freight proportionally based on purchase contribution.

Example:
Vendor total purchase = $1000
Product A purchases = $200
Product B purchases = $800

Freight $100 becomes:
A → $20
B → $80
-----------------------------------------------------------------------------*/

freight_alloc AS (

SELECT
    s.*,

    f.freight_cost *
    (s.total_purchase_amount /
     SUM(s.total_purchase_amount) OVER (PARTITION BY s.vendor_number))
    AS freight_cost

FROM summary_stg s

LEFT JOIN freight_summary f
    ON s.vendor_number = f.vendor_number
)


/*-----------------------------------------------------------------------------
FINAL DATASET
-------------------------------------------------------------------------------
This output becomes a GOLD analytical dataset for dashboards and analysis.
-----------------------------------------------------------------------------*/

SELECT
    vendor_number,
    vendor_name,
    brand,
    description,

    ROUND(actual_price,2) AS actual_price,
    ROUND(avg_purchase_price,2) AS avg_purchase_price,

    total_purchase_qty,
    total_sales_qty,

    total_purchase_amount,
    total_sales_amount,

    excise_tax,
    ROUND(freight_cost,2) AS freight_cost,
	
	/*-----------------------------------------------------------------------------
	STEP 7 — Business KPIs
	-----------------------------------------------------------------------------*/
   
    -- price advantage vs market
    ROUND(actual_price - avg_purchase_price,2) AS price_discount,

    -- vendor gross profit
    total_sales_amount - total_purchase_amount AS gross_profit,

    -- profit margin
    ROUND((total_sales_amount - total_purchase_amount)
	/ NULLIF(total_sales_amount,0) * 100,2) AS profit_margin,

    -- sell-through rate
    ROUND(total_sales_qty * 1.0 / NULLIF(total_purchase_qty,0) *100,2) AS sell_through_rate


FROM freight_alloc

ORDER BY total_purchase_amount DESC;