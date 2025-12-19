
-- Creating bronze table for data cleaning named br_custinfo inside datawarehouse
create table datawarehouse.br_custinfo
(
	cst_id int null,
    cst_key varchar(50),
    cst_firstname varchar (50),
    cst_lastname varchar(50),
    cst_marital_status varchar(50),
    cst_gndr varchar(50),
    cst_create_date date null
    );
    
-- Creating bronze table for data cleaning named br_prdinfo inside datawarehouse 
   create table datawarehouse.br_prdinfo
(
	prd_id int null,
	prd_key varchar(50),
	prd_nm varchar(50),
	prd_cost int null,
	prd_line varchar(50),
	prd_start_dt datetime null,
	prd_end_dt datetime null
    );
     
-- Creating bronze table for data cleaning named br_salesdetails inside datawarehouse    

create table datawarehouse.br_salesdetails
(	sls_ord_num varchar(50),
	sls_prd_key varchar(50),
	sls_cust_id int null,
	sls_order_dt int null,
	sls_ship_dt int null,
	sls_due_dt int null,
	sls_sales int null,
	sls_quantity int null,
	sls_price int null
)
;


-- Creating bronze table for data cleaning named br_erp_CUST_AZ12 inside datawarehouse
create table datawarehouse.br_erp_CUST_AZ12
(	CID varchar(50),
	BDATE date null,
	GEN varchar(50)
    );
    
-- Creating bronze table for data cleaning named br_erp_LOC_A101 inside datawarehouse 
   create table datawarehouse.br_erp_LOC_A101
(	CID varchar(50),
	CNTRY varchar(50)

    );
     
-- Creating bronze table for data cleaning named br_erp_PX_CAT_G1V2 inside datawarehouse    

create table datawarehouse.br_erp_PX_CAT_G1V2
(	ID varchar(50),
	CAT varchar(50),
	SUBCAT varchar(50),
	MAINTENANCE varchar(50)
)
;

 -- ==============================================================================   
    
-- Creating bronze table for data cleaning named sl_custinfo inside datawarehouse_silver
create table datawarehouse_silver.sl_custinfo
(
	cst_id int null,
    cst_key varchar(50),
    cst_firstname varchar (50),
    cst_lastname varchar(50),
    cst_marital_status varchar(50),
    cst_gndr varchar(50),
    cst_create_date date null
    );
    
-- Creating bronze table for data cleaning named sl_prdinfo inside datawarehouse_silver
   create table datawarehouse_silver.sl_prdinfo
(
	prd_id int null,
	prd_key varchar(50),
	prd_nm varchar(50),
	prd_cost int null,
	prd_line varchar(50),
	prd_start_dt datetime null,
	prd_end_dt datetime null
    );
     
-- Creating bronze table for data cleaning named sl_salesdetails inside datawarehouse_silver    

create table datawarehouse_silver.sl_salesdetails
(	sls_ord_num varchar(50),
	sls_prd_key varchar(50),
	sls_cust_id int null,
	sls_order_dt int null,
	sls_ship_dt int null,
	sls_due_dt int null,
	sls_sales int null,
	sls_quantity int null,
	sls_price int null
)
;
-- --------------------------------------------------------------------------------------
-- Creating bronze table for data cleaning named br_erp_CUST_AZ12 inside datawarehouse
create table datawarehouse_silver.sl_erp_CUST_AZ12
(	CID varchar(50),
	BDATE date null,
	GEN varchar(50)
    );
    
-- Creating bronze table for data cleaning named br_erp_LOC_A101 inside datawarehouse 
   create table datawarehouse_silver.sl_erp_LOC_A101
(	CID varchar(50),
	CNTRY varchar(50)

    );
     
-- Creating bronze table for data cleaning named br_erp_PX_CAT_G1V2 inside datawarehouse    

create table datawarehouse_silver.sl_erp_PX_CAT_G1V2
(	ID varchar(50),
	CAT varchar(50),
	SUBCAT varchar(50),
	MAINTENANCE varchar(50)
)
;

 -- ==============================================================================   
    
-- Creating bronze table for data cleaning named sl_custinfo inside datawarehouse_silver
create table datawarehouse_gold.gd_custinfo
(
	cst_id int null,
    cst_key varchar(50),
    cst_firstname varchar (50),
    cst_lastname varchar(50),
    cst_marital_status varchar(50),
    cst_gndr varchar(50),
    cst_create_date date null
    );
    
-- Creating bronze table for data cleaning named sl_prdinfo inside datawarehouse_silver
   create table datawarehouse_gold.gd_prdinfo
(
	prd_id int null,
	prd_key varchar(50),
	prd_nm varchar(50),
	prd_cost int null,
	prd_line varchar(50),
	prd_start_dt datetime null,
	prd_end_dt datetime null
    );
     
-- Creating bronze table for data cleaning named sl_salesdetails inside datawarehouse_silver    

create table datawarehouse_gold.gd_salesdetails
(	sls_ord_num varchar(50),
	sls_prd_key varchar(50),
	sls_cust_id int null,
	sls_order_dt int null,
	sls_ship_dt int null,
	sls_due_dt int null,
	sls_sales int null,
	sls_quantity int null,
	sls_price int null
)
;

-- --------------------------------------------------------------------------------------
-- Creating bronze table for data cleaning named br_erp_CUST_AZ12 inside datawarehouse
create table datawarehouse_gold.gd_erp_CUST_AZ12
(	CID varchar(50),
	BDATE date null,
	GEN varchar(50)
    );
    
-- Creating bronze table for data cleaning named br_erp_LOC_A101 inside datawarehouse 
   create table datawarehouse_gold.gd_erp_LOC_A101
(	CID varchar(50),
	CNTRY varchar(50)

    );
     
-- Creating bronze table for data cleaning named br_erp_PX_CAT_G1V2 inside datawarehouse    

create table datawarehouse_gold.gd_erp_PX_CAT_G1V2
(	ID varchar(50),
	CAT varchar(50),
	SUBCAT varchar(50),
	MAINTENANCE varchar(50)
)
;
