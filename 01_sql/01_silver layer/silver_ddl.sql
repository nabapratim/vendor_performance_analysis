DROP TABLE silver.purchases;

CREATE TABLE silver.purchases(
	inventory_id  VARCHAR(50),
	store  INT,
	brand  INT,
	description VARCHAR(50),
	size VARCHAR(50),
	vendor_number INT,
	vendor_name VARCHAR(50),
	po_number INT,
	po_date DATE,
	receiving_date DATE,
	invoice_date DATE,
	pay_date  DATE,
	purchase_price NUMERIC(10,2),
	quantity INT,
	dollars NUMERIC(10,2),
	classification VARCHAR(50),
	updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE silver.purchase_prices;

CREATE TABLE silver.purchase_prices(
	brand  INT,
	description VARCHAR(50),
	price NUMERIC(10,2),
	size VARCHAR(50),
	volume VARCHAR(50),
	classification VARCHAR(50),
	purchase_price NUMERIC(10,2),
	vendor_number INT,
	vendor_name VARCHAR(50),
	updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE silver.vendor_invoice;

CREATE TABLE silver.vendor_invoice(
	vendor_number INT,
	vendor_name VARCHAR(50),
	invoice_date DATE,
	po_number INT,
	po_date DATE,
	pay_date  DATE,
	quantity INT,
	dollars NUMERIC(10,2),
	freight NUMERIC(10,2),
	approval VARCHAR(50),
    updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE silver.sales;

CREATE TABLE silver.sales(
    inventory_id  VARCHAR(50),
	store  INT,
	brand  INT,
	description VARCHAR(50),
	size VARCHAR(50),
	sales_quantity INT,
	sales_dollars NUMERIC(10,2),
	sales_price NUMERIC(10,2),
	sales_date DATE,
	volume VARCHAR(50),
	classification VARCHAR(50),
	excise_tax NUMERIC(10,2),
	vendor_no INT,
	vendor_name VARCHAR(50),
	updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE silver.vendor_invoice;

CREATE TABLE silver.vendor_invoice(
	vendor_number INT,
	vendor_name VARCHAR(50),
	invoice_date DATE,
	po_number INT,
	po_date DATE,
	pay_date  DATE,
	quantity INT,
	dollars NUMERIC(10,2),
    freight NUMERIC(10,2),
	approval VARCHAR(50),
	updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

















