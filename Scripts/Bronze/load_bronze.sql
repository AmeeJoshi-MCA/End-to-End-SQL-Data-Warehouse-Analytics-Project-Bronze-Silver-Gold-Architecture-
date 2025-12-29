/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.
	- It inserts data in 6 tabled for 'bronze' schema. 
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN
    BEGIN TRY
	
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	
	SET @batch_start_time = GETDATE();
			
			PRINT '================================================';
			PRINT 'Loading Bronze Layer';
			PRINT '================================================';

			PRINT '------------------------------------------------';
			PRINT 'Loading CRM Tables from csv files';
			PRINT '------------------------------------------------';

			PRINT '>> Truncating Table: bronze.crm_cust_info';
		 	
			---- Table 1 - bronze.crm_cust_info Start

			SET @start_time = GETDATE();
			
			TRUNCATE TABLE bronze.crm_cust_info;

			PRINT '>> Inserting Data Into: bronze.crm_cust_info';

			BULK INSERT Bronze.crm_cust_info
					FROM 'C:\DataEngineerProjects\PortfolioProject\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
					WITH (
						FIRSTROW = 2,
						FIELDTERMINATOR = ',',
						TABLOCK
			);

			PRINT '>> -------------';
			
			SET @end_time = GETDATE();
			
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';

			---- Table 1 - bronze.crm_cust_info End


			---- Table 2 - bronze.crm_prd_info Start
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.crm_prd_info';
		  
			  TRUNCATE TABLE bronze.crm_prd_info;

			PRINT '>> Inserting Data Into: bronze.crm_prd_info';

			BULK INSERT bronze.crm_prd_info
					FROM 'C:\DataEngineerProjects\PortfolioProject\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
					WITH (
						FIRSTROW = 2,
						FIELDTERMINATOR = ',',
						TABLOCK
			);

			SET @end_time = GETDATE();
			
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

			---- Table 2 - bronze.crm_prd_info End

			---- Table 3 - bronze.crm_sales_details Start
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.crm_sales_details';
		
			   TRUNCATE TABLE bronze.crm_sales_details;

			PRINT '>> Inserting Data Into: bronze.crm_sales_details';

			BULK INSERT bronze.crm_sales_details
					FROM 'C:\DataEngineerProjects\PortfolioProject\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
					WITH (
						FIRSTROW = 2,
						FIELDTERMINATOR = ',',
						TABLOCK
			);

			SET @end_time = GETDATE();
			
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

			---- Table 3 - bronze.crm_sales_details End

			PRINT '>> -------------';

			PRINT '------------------------------------------------';
			PRINT 'Loading ERP Tables';
			PRINT '------------------------------------------------';
		
			---- Table 4 - bronze.erp_loc_a101 Start
		    SET @start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.erp_loc_a101';
		  
			  TRUNCATE TABLE bronze.erp_loc_a101;

			PRINT '>> Inserting Data Into: bronze.erp_loc_a101';

			BULK INSERT bronze.erp_loc_a101
					FROM 'C:\DataEngineerProjects\PortfolioProject\sql-data-warehouse-project-main\datasets\source_erp\loc_a101.csv'
					WITH (
						FIRSTROW = 2,
						FIELDTERMINATOR = ',',
						TABLOCK
			);
			SET @end_time = GETDATE();
			
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

			---- Table 4 - bronze.erp_loc_a101 End

			---- Table 5 - bronze.erp_cust_az12 Start

			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.erp_cust_az12';

			TRUNCATE TABLE bronze.erp_cust_az12;

			PRINT '>> Inserting Data Into: bronze.erp_cust_az12';

			BULK INSERT bronze.erp_cust_az12
					FROM 'C:\DataEngineerProjects\PortfolioProject\sql-data-warehouse-project-main\datasets\source_erp\cust_az12.csv'
					WITH (
						FIRSTROW = 2,
						FIELDTERMINATOR = ',',
						TABLOCK
			);
			SET @end_time = GETDATE();
			
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

			---- Table 5 - bronze.erp_cust_az12 End


			---- Table 6 - bronze.erp_px_cat_g1v2 Start

			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';

			TRUNCATE TABLE bronze.erp_px_cat_g1v2;

			PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';

			BULK INSERT bronze.erp_px_cat_g1v2
					FROM 'C:\DataEngineerProjects\PortfolioProject\sql-data-warehouse-project-main\datasets\source_erp\px_cat_g1v2.csv'
					WITH (
						FIRSTROW = 2,
						FIELDTERMINATOR = ',',
						TABLOCK
			);
			SET @end_time = GETDATE();
			
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			
			---- Table 6 - bronze.erp_px_cat_g1v2 end

		  PRINT '>> -------------';

		  SET @batch_end_time = GETDATE();
		  PRINT '=========================================='
		  PRINT 'Loading Bronze Layer is Completed';
		  PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		  PRINT '=========================================='

     End Try

	 Begin Catch
	    PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	 End Catch


End
