LOAD DATA LOCAL INFILE '/Users/suvashkumar/Desktop/Desktop/Suvash Kumar/First_SQL_Project/source_crm/cust_info.csv'
INTO TABLE br_custinfo
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(
    @cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    @cst_create_date
)
SET
    cst_id = NULLIF(@cst_id, ''),
    cst_create_date =
        CASE
            WHEN @cst_create_date IS NULL OR TRIM(@cst_create_date) = '' THEN NULL
            WHEN LEFT(@cst_create_date, 10) = '0000-00-00' THEN NULL
            ELSE STR_TO_DATE(LEFT(@cst_create_date, 10), '%Y-%m-%d')
        END;


truncate table br_prdinfo;
LOAD DATA LOCAL INFILE
'/Users/suvashkumar/Desktop/Desktop/Suvash Kumar/First_SQL_Project/source_crm/prd_info.csv'
INTO TABLE br_prdinfo
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(
    prd_id,
    prd_key,
    prd_nm,
    @prd_cost,
    prd_line,
    @prd_start_dt,
    @prd_end_dt
)
SET
    prd_cost = NULLIF(@prd_cost, ''),
    prd_start_dt = STR_TO_DATE(NULLIF(@prd_start_dt, ''), '%Y-%m-%d'),
    prd_end_dt   = STR_TO_DATE(NULLIF(@prd_end_dt, ''), '%Y-%m-%d');

LOAD DATA LOCAL INFILE
'/Users/suvashkumar/Desktop/Desktop/Suvash Kumar/First_SQL_Project/source_crm/sales_details.csv'
INTO TABLE br_salesdetails
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(
    sls_ord_num ,
	sls_prd_key ,
	sls_cust_id ,
	sls_order_dt ,
	sls_ship_dt ,
	sls_due_dt,
	@sls_sales ,
	sls_quantity, 
	@sls_price 
)
SET
    sls_price = NULLIF(@sls_price, ''),
    sls_sales = NULLIF(@sls_sales, '');
    
load data local infile 
'/Users/suvashkumar/Desktop/Desktop/Suvash Kumar/First_SQL_Project/source_erp/CUST_AZ12.csv'
into table br_erp_CUST_AZ12
fields terminated by ','
enclosed by '"'
lines terminated by '\r\n'
ignore 1 rows
(cid, @bdate, @gen)
SET
bdate = NULLIF(@bdate, ''),
gen   = NULLIF(@gen, '');

load data local infile 
'/Users/suvashkumar/Desktop/Desktop/Suvash Kumar/First_SQL_Project/source_erp/LOC_A101.csv'
into table br_erp_LOC_A101
fields terminated by ','
enclosed by '"'
lines terminated by '\r\n'
ignore 1 rows
(
cid,
cntry
);

load data local infile
'/Users/suvashkumar/Desktop/Desktop/Suvash Kumar/First_SQL_Project/source_erp/PX_CAT_G1V2.csv'
into table br_erp_PX_CAT_G1V2
fields terminated by ','
enclosed by '"'
lines terminated by '\r\n'
ignore 1 rows
(id,
cat,
subcat,
maintenance
);

select * from br_erp_PX_CAT_G1V2;


