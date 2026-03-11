/*==============================================================================*/
--index created to optimize the join between purchases and purchase_prices tables
-- purchases table contain 1M+ rows, and queries are taking a long time to execute without the index, which sigificantly increases query execution time.  
--index unables faster lookups and improves performance
/*==============================================================================*/


--index the join columns
CREATE INDEX idx_purchases_vendor
ON silver.purchases(vendor_number);

CREATE INDEX idx_purchase_prices_vendor
ON silver.purchase_prices(vendor_number);

-- create the index for better grouping
CREATE INDEX idx_purchases_group
ON silver.purchases(vendor_number,brand,vendor_name,purchase_price);

-- create index sales table
CREATE INDEX idx_slaes_vendor
on silver.sales(vendor_no);
--
CREATE INDEX idx_sales_group
on silver.sales(description,brand);