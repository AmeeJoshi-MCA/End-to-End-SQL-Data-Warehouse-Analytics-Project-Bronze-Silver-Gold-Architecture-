/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
select 
		ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key
		ci.cst_id as customer_id,
		ci.cst_key as customer_number,
		ci.cst_firstname as first_name,
		ci.cst_lastname as last_name,
		la.cntry as country,
		ci.cst_marital_status as marital_status,
		CASE 
				WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
				 ELSE COALESCE(ca.gen, 'n/a')  			   -- Fallback to ERP data
        END   AS gender,
		ca.bdate as birth_date,
		ci.cst_create_date as create_date
		
from silver.crm_cust_info ci
  LEFT JOIN silver.erp_cust_az12 ca
  on ca.cid = ci.cst_key

  LEFT JOIN silver.erp_loc_a101 la
  on la.cid = ci.cst_key;
 

 Go

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO


 CREATE VIEW gold.dim_products AS
select 
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
	pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
from Silver.crm_prd_info pn
 LEFT JOIN Silver.erp_px_cat_g1v2 pc
 ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data


Go

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO


CREATE VIEW gold.fact_sales AS
select 
	sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
from Silver.crm_sales_details sd
LEFT JOIN Gold.dim_products pr
ON sd.sls_prd_key=pr.product_number
LEFT JOIN Gold.dim_customers cu
ON sd.sls_cust_id=cu.customer_id

Go