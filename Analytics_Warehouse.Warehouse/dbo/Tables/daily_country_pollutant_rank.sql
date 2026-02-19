CREATE TABLE [dbo].[daily_country_pollutant_rank] (

	[client_day] date NULL, 
	[country_name] varchar(8000) NULL, 
	[pt_id] varchar(8000) NULL, 
	[total_value] float NULL, 
	[rank] int NULL
);