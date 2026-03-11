CREATE OR REPLACE PROCEDURE pipeline.refresh_warehouse()
LANGUAGE plpgsql
AS $$

BEGIN

CALL silver.refresh_silver_tables();

CALL gold.refresh_vendor_performance();

END;

$$;