CREATE TABLE [dbo].[global_and_country_pollutant_avg_yearly] (

	[Level] varchar(7) NOT NULL, 
	[country_name] varchar(8000) NULL, 
	[year] int NULL, 
	[pt_id] varchar(8000) NULL, 
	[pollutant_name] varchar(8000) NULL, 
	[units] varchar(8000) NULL, 
	[avg_value] float NULL
);