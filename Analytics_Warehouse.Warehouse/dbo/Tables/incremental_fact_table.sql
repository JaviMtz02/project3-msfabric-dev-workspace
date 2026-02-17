CREATE TABLE [dbo].[incremental_fact_table] (

	[parent_country_id] varchar(8000) NULL, 
	[record_id] varchar(8000) NULL, 
	[sensor_id] varchar(8000) NULL, 
	[location_id] varchar(8000) NULL, 
	[provider_id] varchar(8000) NULL, 
	[owner_id] varchar(8000) NULL, 
	[pt_id] varchar(8000) NULL, 
	[value] float NULL, 
	[Reading_Start_UTC] datetime2(6) NULL, 
	[Reading_End_UTC] datetime2(6) NULL, 
	[Reading_Start_Local] datetime2(6) NULL, 
	[Reading_End_Local] datetime2(6) NULL, 
	[client_day] date NULL
);