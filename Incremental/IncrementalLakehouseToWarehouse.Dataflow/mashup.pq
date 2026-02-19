[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[Description = "date of the data we need to transfer to gold layer"]
shared p_client_day = "2026-02-17" meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = type text];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "incremental_fact_table_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "parent_country_id", DestinationColumnName = "parent_country_id"], [SourceColumnName = "record_id", DestinationColumnName = "record_id"], [SourceColumnName = "sensor_id", DestinationColumnName = "sensor_id"], [SourceColumnName = "location_id", DestinationColumnName = "location_id"], [SourceColumnName = "provider_id", DestinationColumnName = "provider_id"], [SourceColumnName = "owner_id", DestinationColumnName = "owner_id"], [SourceColumnName = "pt_id", DestinationColumnName = "pt_id"], [SourceColumnName = "value", DestinationColumnName = "value"], [SourceColumnName = "Reading_Start_UTC", DestinationColumnName = "Reading_Start_UTC"], [SourceColumnName = "Reading_End_UTC", DestinationColumnName = "Reading_End_UTC"], [SourceColumnName = "Reading_Start_Local", DestinationColumnName = "Reading_Start_Local"], [SourceColumnName = "Reading_End_Local", DestinationColumnName = "Reading_End_Local"], [SourceColumnName = "client_day", DestinationColumnName = "client_day"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared incremental_fact_table = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "38ea5278-6006-410d-8a76-80fa8b39e1e9"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a580ad55-0f08-47c7-9c2f-761283916f8f"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "incremental_fact_table", ItemKind = "Table"]}[Data],

  // Convert pipeline string parameter to Date
  ConvertedDate = Date.From(p_client_day),

  // Filter table using converted date
  #"Filtered Rows" =
      Table.SelectRows(#"Navigation 2", each [client_day] = ConvertedDate)

in
  #"Filtered Rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "incremental_summary_area_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "parent_country_id", DestinationColumnName = "parent_country_id"], [SourceColumnName = "sensor_id", DestinationColumnName = "sensor_id"], [SourceColumnName = "location_id", DestinationColumnName = "location_id"], [SourceColumnName = "country_code", DestinationColumnName = "country_code"], [SourceColumnName = "Uptime_Start", DestinationColumnName = "Uptime_Start"], [SourceColumnName = "Uptime_End", DestinationColumnName = "Uptime_End"], [SourceColumnName = "Longitude", DestinationColumnName = "Longitude"], [SourceColumnName = "Latitude", DestinationColumnName = "Latitude"], [SourceColumnName = "client_day", DestinationColumnName = "client_day"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared incremental_summary_area = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "38ea5278-6006-410d-8a76-80fa8b39e1e9"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a580ad55-0f08-47c7-9c2f-761283916f8f"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "incremental_summary_area", ItemKind = "Table"]}[Data],

  // Convert string parameter to Date
  ConvertedDate = Date.From(p_client_day),

  // Apply filter
  #"Filtered Rows" =
      Table.SelectRows(#"Navigation 2", each [client_day] = ConvertedDate)

in
  #"Filtered Rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "summary_pollutant_hour_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "parent_country_id", DestinationColumnName = "parent_country_id"], [SourceColumnName = "country_name", DestinationColumnName = "country_name"], [SourceColumnName = "Area", DestinationColumnName = "Area"], [SourceColumnName = "locality", DestinationColumnName = "locality"], [SourceColumnName = "timezone", DestinationColumnName = "timezone"], [SourceColumnName = "hour_utc", DestinationColumnName = "hour_utc"], [SourceColumnName = "hour_local", DestinationColumnName = "hour_local"], [SourceColumnName = "units", DestinationColumnName = "units"], [SourceColumnName = "pollutant_name", DestinationColumnName = "pollutant_name"], [SourceColumnName = "pt_id", DestinationColumnName = "pt_id"], [SourceColumnName = "client_day", DestinationColumnName = "client_day"], [SourceColumnName = "value", DestinationColumnName = "value"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared summary_pollutant_hour = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "38ea5278-6006-410d-8a76-80fa8b39e1e9"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a580ad55-0f08-47c7-9c2f-761283916f8f"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "summary_pollutant_hour", ItemKind = "Table"]}[Data],

  // Convert pipeline string parameter to Date
  ConvertedDate = Date.From(p_client_day),

  // Filter by client_day
  #"Filtered Rows" =
      Table.SelectRows(#"Navigation 2", each [client_day] = ConvertedDate)

in
  #"Filtered Rows";
shared incremental_fact_table_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "38ea5278-6006-410d-8a76-80fa8b39e1e9"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "29c66db1-0b10-49b5-bf38-c4f650dcd097"]}[Data],
  TableNavigation = Navigation_2{[Item = "incremental_fact_table", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared incremental_summary_area_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "38ea5278-6006-410d-8a76-80fa8b39e1e9"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "29c66db1-0b10-49b5-bf38-c4f650dcd097"]}[Data],
  TableNavigation = Navigation_2{[Item = "incremental_summary_area", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared summary_pollutant_hour_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "38ea5278-6006-410d-8a76-80fa8b39e1e9"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "29c66db1-0b10-49b5-bf38-c4f650dcd097"]}[Data],
  TableNavigation = Navigation_2{[Item = "summary_pollutant_hour", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "sensor_count_daily_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "client_day", DestinationColumnName = "client_day"], [SourceColumnName = "country_name", DestinationColumnName = "country_name"], [SourceColumnName = "pt_id", DestinationColumnName = "pt_id"], [SourceColumnName = "total_sensors", DestinationColumnName = "total_sensors"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared sensor_count_daily = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "38ea5278-6006-410d-8a76-80fa8b39e1e9"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a580ad55-0f08-47c7-9c2f-761283916f8f"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "sensor_count_daily", ItemKind = "Table"]}[Data],

  // Convert pipeline string parameter to Date
  ConvertedDate = Date.From(p_client_day),

  // Filter by client_day
  #"Filtered Rows" =
      Table.SelectRows(#"Navigation 2", each [client_day] = ConvertedDate)

in
  #"Filtered Rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "daily_country_pollutant_rank_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "client_day", DestinationColumnName = "client_day"], [SourceColumnName = "country_name", DestinationColumnName = "country_name"], [SourceColumnName = "pt_id", DestinationColumnName = "pt_id"], [SourceColumnName = "total_value", DestinationColumnName = "total_value"], [SourceColumnName = "rank", DestinationColumnName = "rank"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared daily_country_pollutant_rank = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "38ea5278-6006-410d-8a76-80fa8b39e1e9"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a580ad55-0f08-47c7-9c2f-761283916f8f"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "daily_country_pollutant_rank", ItemKind = "Table"]}[Data],

  // Convert string parameter to Date
  ConvertedDate = Date.From(p_client_day),

  // Filter by client_day
  #"Filtered Rows" =
      Table.SelectRows(#"Navigation 2", each [client_day] = ConvertedDate)

in
  #"Filtered Rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "daily_top_polluted_areas_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "client_day", DestinationColumnName = "client_day"], [SourceColumnName = "country_name", DestinationColumnName = "country_name"], [SourceColumnName = "area", DestinationColumnName = "area"], [SourceColumnName = "pt_id", DestinationColumnName = "pt_id"], [SourceColumnName = "total_value", DestinationColumnName = "total_value"], [SourceColumnName = "rank", DestinationColumnName = "rank"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared daily_top_polluted_areas = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "38ea5278-6006-410d-8a76-80fa8b39e1e9"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a580ad55-0f08-47c7-9c2f-761283916f8f"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "daily_top_polluted_areas", ItemKind = "Table"]}[Data],

  // Convert pipeline string parameter to Date
  ConvertedDate = Date.From(p_client_day),

  // Filter by client_day
  #"Filtered Rows" =
      Table.SelectRows(#"Navigation 2", each [client_day] = ConvertedDate)

in
  #"Filtered Rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dod_pollutant_analytics_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "client_day", DestinationColumnName = "client_day"], [SourceColumnName = "country_name", DestinationColumnName = "country_name"], [SourceColumnName = "pt_id", DestinationColumnName = "pt_id"], [SourceColumnName = "pollutant_name", DestinationColumnName = "pollutant_name"], [SourceColumnName = "avg_value", DestinationColumnName = "avg_value"], [SourceColumnName = "min_value", DestinationColumnName = "min_value"], [SourceColumnName = "max_value", DestinationColumnName = "max_value"], [SourceColumnName = "prev_avg_value", DestinationColumnName = "prev_avg_value"], [SourceColumnName = "dod_pct_change", DestinationColumnName = "dod_pct_change"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared dod_pollutant_analytics = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "38ea5278-6006-410d-8a76-80fa8b39e1e9"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a580ad55-0f08-47c7-9c2f-761283916f8f"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dod_pollutant_analytics", ItemKind = "Table"]}[Data],

  // Convert string parameter to Date
  ConvertedDate = Date.From(p_client_day),

  // Filter by client_day
  #"Filtered Rows" =
      Table.SelectRows(#"Navigation 2", each [client_day] = ConvertedDate)

in
  #"Filtered Rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "provider_count_daily_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "client_day", DestinationColumnName = "client_day"], [SourceColumnName = "country_name", DestinationColumnName = "country_name"], [SourceColumnName = "pt_id", DestinationColumnName = "pt_id"], [SourceColumnName = "total_sensors", DestinationColumnName = "total_sensors"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared provider_count_daily = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "38ea5278-6006-410d-8a76-80fa8b39e1e9"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a580ad55-0f08-47c7-9c2f-761283916f8f"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "provider_count_daily", ItemKind = "Table"]}[Data],

  // Convert string parameter to Date
  ConvertedDate = Date.From(p_client_day),

  // Filter by client_day
  #"Filtered Rows" =
      Table.SelectRows(#"Navigation 2", each [client_day] = ConvertedDate)

in
  #"Filtered Rows";
shared sensor_count_daily_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "38ea5278-6006-410d-8a76-80fa8b39e1e9"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "29c66db1-0b10-49b5-bf38-c4f650dcd097"]}[Data],
  TableNavigation = Navigation_2{[Item = "sensor_count_daily", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared daily_country_pollutant_rank_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "38ea5278-6006-410d-8a76-80fa8b39e1e9"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "29c66db1-0b10-49b5-bf38-c4f650dcd097"]}[Data],
  TableNavigation = Navigation_2{[Item = "daily_country_pollutant_rank", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared daily_top_polluted_areas_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "38ea5278-6006-410d-8a76-80fa8b39e1e9"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "29c66db1-0b10-49b5-bf38-c4f650dcd097"]}[Data],
  TableNavigation = Navigation_2{[Item = "daily_top_polluted_areas", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared dod_pollutant_analytics_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "38ea5278-6006-410d-8a76-80fa8b39e1e9"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "29c66db1-0b10-49b5-bf38-c4f650dcd097"]}[Data],
  TableNavigation = Navigation_2{[Item = "dod_pollutant_analytics", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared provider_count_daily_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "38ea5278-6006-410d-8a76-80fa8b39e1e9"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "29c66db1-0b10-49b5-bf38-c4f650dcd097"]}[Data],
  TableNavigation = Navigation_2{[Item = "provider_count_daily", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "daily_pollutant_threshhold_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "country_name", DestinationColumnName = "country_name"], [SourceColumnName = "pt_id", DestinationColumnName = "pt_id"], [SourceColumnName = "value", DestinationColumnName = "value"], [SourceColumnName = "client_day", DestinationColumnName = "client_day"], [SourceColumnName = "above_threshold", DestinationColumnName = "above_threshold"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared daily_pollutant_threshhold = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "38ea5278-6006-410d-8a76-80fa8b39e1e9"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a580ad55-0f08-47c7-9c2f-761283916f8f"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "daily_pollutant_threshhold", ItemKind = "Table"]}[Data],

  // Convert pipeline string parameter to Date
  ConvertedDate = Date.From(p_client_day),

  // Filter by client_day
  #"Filtered Rows" =
      Table.SelectRows(#"Navigation 2", each [client_day] = ConvertedDate)

in
  #"Filtered Rows";
shared daily_pollutant_threshhold_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "38ea5278-6006-410d-8a76-80fa8b39e1e9"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "29c66db1-0b10-49b5-bf38-c4f650dcd097"]}[Data],
  TableNavigation = Navigation_2{[Item = "daily_pollutant_threshhold", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
