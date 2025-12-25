
-- 1. Create Customer Dimension first
CREATE OR REPLACE VIEW datawarehouse_gold.dim_customers AS 
SELECT
    ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    lo.cntry AS country,
    ci.cst_marital_status AS marital_status,
    CASE WHEN cst_gndr != 'N/A' THEN ci.cst_gndr 
         ELSE COALESCE(ck.gen, 'N/A')
    END AS gender,
    ck.BDATE AS birthdate,
    ci.cst_create_date AS create_date
FROM datawarehouse_silver.sl_custinfo ci
LEFT JOIN datawarehouse_silver.sl_erp_CUST_AZ12 ck ON ci.cst_key = ck.CID
LEFT JOIN datawarehouse_silver.sl_erp_LOC_A101 lo ON ci.cst_key = lo.CID;

-- 2. Create Product Dimension second
CREATE OR REPLACE VIEW datawarehouse_gold.dim_products AS
SELECT 
    ROW_NUMBER() OVER(ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key,
    pi.prd_id AS product_id,
    pi.prd_key AS product_number,
    pi.prd_nm AS product_name,
    pi.cat_id AS category_id,
    pc.cat AS catagory,
    pc.subcat AS subcatagory,
    pc.maintenance,
    pi.prd_cost AS cost,
    pi.prd_line AS product_line,
    pi.prd_start_dt AS start_date
FROM datawarehouse_silver.sl_prdinfo pi
LEFT JOIN datawarehouse_silver.sl_erp_PX_CAT_G1V2 pc ON pi.cat_id = pc.id
WHERE prd_end_dt IS NULL;

-- 3. Create Fact Sales last (it joins the two views above)
CREATE OR REPLACE VIEW datawarehouse_gold.fact_sales AS 
SELECT 
    sd.sls_ord_num AS order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shiping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM datawarehouse_silver.sl_salesdetails sd
LEFT JOIN datawarehouse_gold.dim_products pr ON sd.sls_prd_key = pr.product_number
LEFT JOIN datawarehouse_gold.dim_customers cu ON sd.sls_cust_id = cu.customer_id;
