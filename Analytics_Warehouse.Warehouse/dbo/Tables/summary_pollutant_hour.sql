CREATE TABLE [dbo].[summary_pollutant_hour] (

	[parent_country_id] varchar(8000) NULL, 
	[country_name] varchar(8000) NULL, 
	[Area] varchar(8000) NULL, 
	[locality] varchar(8000) NULL, 
	[timezone] varchar(8000) NULL, 
	[hour_utc] int NULL, 
	[hour_local] int NULL, 
	[units] varchar(8000) NULL, 
	[pollutant_name] varchar(8000) NULL, 
	[pt_id] varchar(8000) NULL, 
	[client_day] date NULL, 
	[value] float NULL
);