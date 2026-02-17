[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[Description = "date of the data we need to transfer to gold layer"]
shared p_client_day = "2026-02-16" meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = type text];
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
