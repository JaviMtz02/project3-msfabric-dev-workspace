CREATE TABLE [dbo].[dod_pollutant_analytics] (

	[client_day] date NULL, 
	[country_name] varchar(8000) NULL, 
	[pt_id] varchar(8000) NULL, 
	[pollutant_name] varchar(8000) NULL, 
	[avg_value] float NULL, 
	[min_value] float NULL, 
	[max_value] float NULL, 
	[prev_avg_value] float NULL, 
	[dod_pct_change] float NULL
);