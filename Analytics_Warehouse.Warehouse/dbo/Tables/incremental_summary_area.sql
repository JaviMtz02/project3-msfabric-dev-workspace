CREATE TABLE [dbo].[incremental_summary_area] (

	[country_name] varchar(8000) NULL, 
	[country_code] varchar(8000) NULL, 
	[sensor_id] varchar(8000) NULL, 
	[location_id] varchar(8000) NULL, 
	[Uptime_Start] datetime2(6) NULL, 
	[Uptime_End] datetime2(6) NULL, 
	[Longitude] float NULL, 
	[Latitude] float NULL, 
	[client_day] date NULL
);