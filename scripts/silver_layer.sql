
DELIMITER $$

CREATE PROCEDURE datawarehouse_silver.load_silver_layer()
BEGIN
    /* =========================================================
       1. CRM CUSTOMER INFO → SILVER
       ========================================================= */
    SELECT '>> Truncating & Load sl_custinfo' AS log;
    TRUNCATE TABLE datawarehouse_silver.sl_custinfo;

    INSERT INTO datawarehouse_silver.sl_custinfo (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname)  AS cst_lastname,
        CASE
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'N/A'
        END AS cst_marital_status,
        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'N/A'
        END AS cst_gndr,
        cst_create_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY cst_id
                ORDER BY cst_create_date DESC
            ) AS flag
        FROM datawarehouse.br_custinfo
    ) t
    WHERE flag = 1;

    /* =========================================================
       2. CRM PRODUCT INFO → SILVER
       ========================================================= */
    SELECT '>> Truncating & Load sl_prdinfo' AS log;
    TRUNCATE TABLE datawarehouse_silver.sl_prdinfo;

    INSERT INTO datawarehouse_silver.sl_prdinfo (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7)                     AS prd_key,
        prd_nm,
        COALESCE(prd_cost, 0)                     AS prd_cost,
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Others Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'N/A'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        CAST(
            LEAD(prd_start_dt) OVER (
                PARTITION BY prd_key
                ORDER BY prd_start_dt
            ) - INTERVAL 1 DAY AS DATE
        ) AS prd_end_dt
    FROM datawarehouse.br_prdinfo;

    /* =========================================================
       3. CRM SALES DETAILS → SILVER
       ========================================================= */
    SELECT '>> Truncating & Load sl_salesdetails' AS log;
    TRUNCATE TABLE datawarehouse_silver.sl_salesdetails;

    INSERT INTO datawarehouse_silver.sl_salesdetails (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE
            WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) <> 8 THEN NULL
            ELSE CAST(sls_order_dt AS DATE)
        END AS sls_order_dt,
        CASE
            WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) <> 8 THEN NULL
            ELSE CAST(sls_ship_dt AS DATE)
        END AS sls_ship_dt,
        CASE
            WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) <> 8 THEN NULL
            ELSE CAST(sls_due_dt AS DATE)
        END AS sls_due_dt,
        CASE
            WHEN sls_sales IS NULL
              OR sls_sales <= 0
              OR sls_sales <> sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE
            WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE ROUND(sls_price / NULLIF(sls_quantity, 0), 2)
        END AS sls_price
    FROM datawarehouse.br_salesdetails;

    /* =========================================================
       4. ERP CUSTOMER AZ12 → SILVER
       ========================================================= */
    SELECT '>> Truncating & Load sl_erp_cust_az12' AS log;
    TRUNCATE TABLE datawarehouse_silver.sl_erp_cust_az12;

    INSERT INTO datawarehouse_silver.sl_erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4)
            ELSE cid
        END AS cid,
        CASE
            WHEN bdate > CURDATE() THEN NULL
            ELSE bdate
        END AS bdate,
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE')   THEN 'Male'
            ELSE 'N/A'
        END AS gen
    FROM datawarehouse.br_erp_cust_az12;

    /* =========================================================
       5. ERP LOCATION A101 → SILVER
       ========================================================= */
    SELECT '>> Truncating & Load sl_erp_loc_a101' AS log;
    TRUNCATE TABLE datawarehouse_silver.sl_erp_loc_a101;

    INSERT INTO datawarehouse_silver.sl_erp_loc_a101 (
        cid,
        cntry
    )
    SELECT
        REPLACE(cid, '-', '') AS cid,
        CASE
            WHEN TRIM(cntry) = 'DE'            THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA')  THEN 'United States'
            WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'N/A'
            ELSE TRIM(cntry)
        END AS cntry
    FROM datawarehouse.br_erp_loc_a101;

    /* =========================================================
       6. ERP PRICE CATEGORY → SILVER
       ========================================================= */
    SELECT '>> Truncating & Load sl_erp_px_cat_g1v2' AS log;
    TRUNCATE TABLE datawarehouse_silver.sl_erp_px_cat_g1v2;

    INSERT INTO datawarehouse_silver.sl_erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM datawarehouse.br_erp_px_cat_g1v2;

END $$
DELIMITER ;


