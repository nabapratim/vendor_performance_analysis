CREATE OR REPLACE PROCEDURE silver.refresh_silver_tables()
LANGUAGE plpgsql
AS $$

BEGIN

/*--------------------------------------------------
Refresh purchases
--------------------------------------------------*/


------ Insert data into silver.purchases from bronze.stg_purchases with necessary transformation.
INSERT INTO silver.purchases(
	inventory_id,
	store,
	brand,
	description,
	size,
	vendor_number,
	vendor_name,
	po_number,
	po_date,
	receiving_date,
	invoice_date,
	pay_date,
	purchase_price,
	quantity,
	dollars,
	classification,
	updated_at
)
	select
		cast("InventoryId" as varchar(50)) as inventory_id,
		cast("Store" as int) as store,
		cast("Brand" as int) as brand,
		cast(trim("Description") as varchar(50)) as description,
		cast("Size" as varchar(50)) as size,
		cast("VendorNumber" as int) as vendor_number,
		cast(trim("VendorName") as varchar(50)) as vendor_name,
		cast("PONumber" as int) as po_number,
		cast("PODate" as DATE) as po_date,
		cast("ReceivingDate" as DATE) as recieving_date,
		cast("InvoiceDate" as DATE) as invoice_date,
		cast("PayDate" as DATE) as pay_date,
		cast("PurchasePrice" as numeric(10,2)) as purchase_price,
		cast("Quantity" as int) as quantity,
		cast("Dollars" as numeric(10,2)) as dollars,
		cast("Classification" as varchar(50)) as classification,
		CURRENT_TIMESTAMP AS updated_at
	from bronze.stg_purchases
	WHERE  "Quantity" IS NOT NULL
		AND "Dollars" IS NOT NULL
		AND "Quantity"::INT > 0
		AND "Dollars"::NUMERIC > 0;

---insert into silver.purchase_prices from bronze.stg_purchase_prices with necessary trnsformation.
INSERT INTO silver.purchase_prices(
	brand,
	description, 
	price,
	size,
	volume, 
	classification, 
	purchase_price, 
	vendor_number, 
	vendor_name, 
	updated_at 
)
select
		
		cast("Brand" as int) as brand,
		cast(trim("Description") as varchar(50)) as description,
		cast("Price" as numeric(10,2)) as price,
		cast("Size" as varchar(50)) as size,
        cast("Volume" as varchar(50)) as volume,
		cast("Classification" as varchar(50)) as classification,
		cast("PurchasePrice" as numeric(10,2)) as purchase_price,
		cast("VendorNumber" as int) as vendor_number,
		cast(trim("VendorName") as varchar(50)) as vendor_name,
		CURRENT_TIMESTAMP AS updated_at
	from bronze.stg_purchase_prices
	WHERE  "Price" IS NOT NULL
	    AND "PurchasePrice" IS NOT NULL
		AND "VendorNumber" IS NOT NULL
		AND "Price"::INT > 0
		AND "PurchasePrice"::NUMERIC > 0;

---insert into silver.sales from bronze.stg_sales with necessary trnsformation.
INSERT INTO silver.sales(
   inventory_id ,
	store,
	brand,
	description,
	size,
	sales_quantity,
	sales_dollars,
	sales_price,
	sales_date,
	volume,
	classification,
	excise_tax,
	vendor_no,
	vendor_name,
	updated_at
)
	select
		cast("InventoryId" as varchar(50)) as inventory_id,
		cast("Store" as int) as store,
		cast("Brand" as int) as brand,
		cast(trim("Description") as varchar(50)) as description,
		cast("Size" as varchar(50)) as size,
		cast("SalesQuantity" as int) as sales_quantity,
		cast("SalesDollars" as numeric(10,2)) as sales_dollars,
		cast("SalesPrice" as numeric(10,2)) as SalesPrice,
		cast("SalesDate" as DATE) as sales_date,
		cast("Volume" as VARCHAR(50)) as volume,
		cast("Classification" as varchar(50)) as classification,
		cast("ExciseTax" as numeric(10,2)) as excise_tax,
		cast("VendorNo" as int) as vendor_no,
		cast(trim("VendorName") as varchar(50)) as vendor_name,
		CURRENT_TIMESTAMP AS updated_at
	from bronze.stg_sales
	WHERE  "SalesQuantity" IS NOT NULL
		AND "SalesDollars" IS NOT NULL
		AND "SalesQuantity"::INT > 0
		AND "SalesDollars"::NUMERIC > 0;

-----insert into silver.vendor_invoice from bronze.stg_vendor_invoice with necessary trnsformation.
INSERT INTO silver.vendor_invoice(
	vendor_number,
	vendor_name,
	invoice_date ,
	po_number,
	po_date ,
	pay_date ,
	quantity,
	dollars,
    freight,
	approval,
	updated_at
)
   select
		cast("VendorNumber" as int) as vendor_number,
		cast(trim("VendorName") as varchar(50)) as vendor_name,
		cast("InvoiceDate" as DATE) as invoice_date,
		cast("PONumber" as int) as po_number,
		cast("PODate" as DATE) as po_date,
		cast("PayDate" as DATE) as pay_date,		
		cast("Quantity" as int) as quantity,
		cast("Dollars" as numeric(10,2)) as dollars,
		cast("Freight" as numeric(10,2)) as freight,
		cast("Approval"as varchar(50)) as approval,
		CURRENT_TIMESTAMP AS updated_at
	from bronze.stg_vendor_invoice
	WHERE  "Quantity" IS NOT NULL
		AND "Dollars" IS NOT NULL
		AND "Quantity"::INT > 0
		AND "Dollars"::NUMERIC > 0;

END;

$$;

