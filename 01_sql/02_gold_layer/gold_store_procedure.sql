/*==============================================================
Stored Procedure
Refresh materialized view
==============================================================*/

CREATE OR REPLACE PROCEDURE gold.refresh_vendor_performance()
LANGUAGE plpgsql
AS $$

BEGIN

REFRESH MATERIALIZED VIEW gold.vendor_performance;

END;

$$;