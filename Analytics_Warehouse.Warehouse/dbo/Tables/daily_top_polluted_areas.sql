CREATE TABLE [dbo].[daily_top_polluted_areas] (

	[client_day] date NULL, 
	[country_name] varchar(8000) NULL, 
	[area] varchar(8000) NULL, 
	[pt_id] varchar(8000) NULL, 
	[total_value] float NULL, 
	[rank] int NULL
);