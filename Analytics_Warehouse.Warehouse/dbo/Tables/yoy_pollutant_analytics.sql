CREATE TABLE [dbo].[yoy_pollutant_analytics] (

	[pollutant_name] varchar(8000) NULL, 
	[pt_id] varchar(8000) NULL, 
	[units] varchar(8000) NULL, 
	[year] int NULL, 
	[country_name] varchar(8000) NULL, 
	[avg_value] float NULL, 
	[min_value] float NULL, 
	[max_value] float NULL, 
	[YoY_change] float NULL
);