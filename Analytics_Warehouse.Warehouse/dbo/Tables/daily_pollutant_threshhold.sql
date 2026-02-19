CREATE TABLE [dbo].[daily_pollutant_threshhold] (

	[country_name] varchar(8000) NULL, 
	[pt_id] varchar(8000) NULL, 
	[value] float NULL, 
	[client_day] date NULL, 
	[above_threshold] bit NULL
);