[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Append"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Fabric.Warehouse([CreateNavigationProperties = false]){[workspaceId = "38ea5278-6006-410d-8a76-80fa8b39e1e9"]}[Data]{[warehouseId = "29c66db1-0b10-49b5-bf38-c4f650dcd097"]}[Data];
[BindToDefaultDestination = true]
shared dim_location = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "38ea5278-6006-410d-8a76-80fa8b39e1e9"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a580ad55-0f08-47c7-9c2f-761283916f8f"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_location", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared dim_owner = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "38ea5278-6006-410d-8a76-80fa8b39e1e9"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a580ad55-0f08-47c7-9c2f-761283916f8f"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_owner", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared dim_pollutant = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "38ea5278-6006-410d-8a76-80fa8b39e1e9"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a580ad55-0f08-47c7-9c2f-761283916f8f"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_pollutant", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared dim_provider = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "38ea5278-6006-410d-8a76-80fa8b39e1e9"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a580ad55-0f08-47c7-9c2f-761283916f8f"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_provider", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared fact_table = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "38ea5278-6006-410d-8a76-80fa8b39e1e9"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a580ad55-0f08-47c7-9c2f-761283916f8f"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "fact_table", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared summary_area = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "38ea5278-6006-410d-8a76-80fa8b39e1e9"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a580ad55-0f08-47c7-9c2f-761283916f8f"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "summary_area", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared summary_pollutant_year = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "38ea5278-6006-410d-8a76-80fa8b39e1e9"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a580ad55-0f08-47c7-9c2f-761283916f8f"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "summary_pollutant_year", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
