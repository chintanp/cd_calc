# This file helps running the queries against the PG DB.


## Charging distance algorithm ( a long process ~ 1-5 days, only need to run once)

# Find the charging stations in the base case. 
# 1.  Find the shortest path betweeen the OD pair. 
# 2.  Find the charging stations within a distance of 10 miles from the shortest path (st_dwithin)
# 3.  Find the points on the shortest path near the relevant charging stations (  relevant are determined from step-2).
# 4.  Find the ratios of the points wrt to the start of the shortest path. So, origin is 0 and destination is 1 and charging station ratios belong to [0,1].
# 5.  In R, from the ratios, find the difference of consecutive ratios and determine the index of the maximum value. This multiplied by the length of the shortest path is the charging distance. 
# 6.  With the index of the maximum difference causing ratio, find the one before and this represents the geometry with long charging distance. 
# 7.  The charging distance table consists of the charging distance and the corresponding geometry for each OD pair for ChaDEMO and COMBO.

# Updating the charging distance (a shorter process ~ 1-10 mins, to be run everytime a new set of charging stations are entered)
# 1.  Find the geometries from the WA_roads table closest to the points. 
# 2.  Search for this geometry in the charging distance table geometry column for intersection. 
# 3.  Update the charging distances for each OD pair that tests true for intersection. 


library(DBI)
library(RPostgres)
library(readr)
library(dplyr)

LOOKUP_DISTANCE <- 10 # in miles, deviation from the road

con <-
  DBI::dbConnect(
    RPostgres::Postgres(),
    user = "postgres",
    password = Sys.getenv("PG_PWD"),
    host = "127.0.0.1",
    port = 5432,
    dbname = "wsdot_evse_sp"
  )

# Read the EVSE information through the AFDC API
afdc_url  <-
  paste0(
    "https://developer.nrel.gov/api/alt-fuel-stations/v1.csv?fuel_type=ELEC&state=WA&ev_charging_level=dc_fast&status=E&access=public&api_key=",
    Sys.getenv('AFDC_API_KEY')
  )
evse_dcfc <- vroom::vroom(afdc_url, delim = ",")
nevses <- nrow(evse_dcfc)


evse_dcfc <-
  tibble::add_column(evse_dcfc,
                     EV_Connector_Code = 0,
                     ChargingCost = 0)


# Convert the connector type to code for easy parsing in GAMA
# CHADEMO only - 1
# J1772COMBO only - 2
# CHADEMO and J1772COMBO - 3
# TESLA - 4
# Ignore J1772 as it is level-2
for (i in 1:nrow(evse_dcfc)) {
  conns <- evse_dcfc$`EV Connector Types`[i]
  if (grepl("CHADEMO", conns)) {
    if (grepl("J1772COMBO", conns)) {
      evse_dcfc$EV_Connector_Code[i] <- 3
    } else {
      evse_dcfc$EV_Connector_Code[i] <- 1
    }
  } else if (grepl("J1772COMBO", conns)) {
    evse_dcfc$EV_Connector_Code[i] <- 2
  } else if (grepl("TESLA", conns)) {
    evse_dcfc$EV_Connector_Code[i] <- 4
  }
}

all_trips_nz_df <-
  vroom::vroom("data-raw/all_trips_non_zero.csv", delim = ",")

all_trips_nz_df <-
  tibble::add_column(all_trips_nz_df, cd_chademo = 0, cd_combo = 0)

# DBI::dbWriteTable(
#   con,
#   "evse_dcfc",
#   evse_dcfc,
#   row.names = FALSE,
#   append = FALSE,
#   overwrite = TRUE
# )
#
# profvis::profvis(for (trip_count in 1:100) {
#   query_chademo <- paste0(
#     'select distinct st_linelocatepoint(line, pta) as ratios
#     from
#     (SELECT
#     sp_od(',
#     all_trips_nz_df$origin[trip_count],
#     ',',
#     all_trips_nz_df$destination[trip_count],
#     ') as line,
#     ST_SetSRID(ST_MakePoint("Longitude", "Latitude" ), 4326) as pta
#     from evse_dcfc where ("EV_Connector_Code" = 1 OR "EV_Connector_Code" = 3) AND st_dwithin(ST_GeogFromWKB(ST_SetSRID(ST_MakePoint("Longitude", "Latitude" ), 4326)), ST_GeogFromWKB((select sp_od(',
#     all_trips_nz_df$origin[trip_count],
#     ',',
#     all_trips_nz_df$destination[trip_count],
#     '))), cast(',
#     LOOKUP_DISTANCE,
#     '/0.000621371 as float))) as data ORDER BY ratios ASC'
#     )
#
#   res_chademo <-
#     DBI::dbSendQuery(con, query_chademo)
#   nr_chademo <- DBI::dbFetch(res_chademo)
#   DBI::dbClearResult(res_chademo)
#
#   max_nr_chademo <- max(diff(nr_chademo$ratios))
#
#   query_combo <- paste0(
#     'select distinct st_linelocatepoint(line, pta) as ratios
#     from
#     (SELECT
#     sp_od(',
#     all_trips_nz_df$origin[trip_count],
#     ',',
#     all_trips_nz_df$destination[trip_count],
#     ') as line,
#     ST_SetSRID(ST_MakePoint("Longitude", "Latitude" ), 4326) as pta
#     from evse_dcfc where ("EV_Connector_Code" = 2 OR "EV_Connector_Code" = 3) AND st_dwithin(ST_GeogFromWKB(ST_SetSRID(ST_MakePoint("Longitude", "Latitude" ), 4326)), ST_GeogFromWKB((select sp_od(',
#     all_trips_nz_df$origin[trip_count],
#     ',',
#     all_trips_nz_df$destination[trip_count],
#     '))), cast(',
#     LOOKUP_DISTANCE,
#     '/0.000621371 as float))) as data ORDER BY ratios ASC'
#     )
#
#   res_combo <-
#     DBI::dbSendQuery(con, query_combo)
#   nr_combo <- DBI::dbFetch(res_combo)
#   DBI::dbClearResult(res_combo)
#
#   max_nr_combo <- max(diff(nr_combo$ratios))
#
#   cd_chademo <- NULL
#
#   query <- paste0(
#     'SELECT
#     ST_Length(ST_LineSubstring(
#     sp_od(',
#     all_trips_nz_df$origin[trip_count],
#     ',',
#     all_trips_nz_df$destination[trip_count],
#     '),',
#     0,
#     ',',
#     max_nr_chademo,
#     ')::geography)
#     '
#     )
#
#   res <- DBI::dbSendQuery(con, query)
#   cd_chademo <- DBI::dbFetch(res)
#   DBI::dbClearResult(res)
#
#
#   cd_combo <- NULL
#
#   query <- paste0(
#     'SELECT
#     ST_Length(ST_LineSubstring(
#     sp_od(',
#     all_trips_nz_df$origin[trip_count],
#     ',',
#     all_trips_nz_df$destination[trip_count],
#     '),',
#     0,
#     ',',
#     max_nr_combo,
#     ')::geography)
#     '
#     )
#
#   res <- DBI::dbSendQuery(con, query)
#   cd_combo <- DBI::dbFetch(res)
#   DBI::dbClearResult(res)
#
#
#   all_trips_nz_df$cd_chademo[trip_count] <- cd_chademo$st_length
#   all_trips_nz_df$cd_combo[trip_count] <- cd_combo$st_length
# })
#
# profvis::profvis({
# query_chademo <- paste0(
#   'select st_length(ST_LineSubstring(
#     sp_od(',
#   all_trips_nz_df$origin[trip_count],
#   ',',
#   all_trips_nz_df$destination[trip_count],
#   '),', 0, ', max(deltas))::geography) from (select foo.ratios - lag(foo.ratios) over() as deltas from (select distinct st_linelocatepoint(line, pta) as ratios
#   from
#   (SELECT
#   sp_od(',
#   all_trips_nz_df$origin[trip_count],
#   ',',
#   all_trips_nz_df$destination[trip_count],
#   ') as line,
#   ST_SetSRID(ST_MakePoint("Longitude", "Latitude" ), 4326) as pta
#   from evse_dcfc where ("EV_Connector_Code" = 1 OR "EV_Connector_Code" = 3) AND st_dwithin(ST_GeogFromWKB(ST_SetSRID(ST_MakePoint("Longitude", "Latitude" ), 4326)), ST_GeogFromWKB((select sp_od(',
#   all_trips_nz_df$origin[trip_count],
#   ',',
#   all_trips_nz_df$destination[trip_count],
#   '))), cast(',
#   LOOKUP_DISTANCE,
#   '/0.000621371 as float))) as data ORDER BY ratios ASC) as foo) as foor'
#   )

# res_chademo <-
#   DBI::dbSendQuery(con, query_chademo)
# cd_chademo <- DBI::dbFetch(res_chademo)
# DBI::dbClearResult(res_chademo)})
cd_chademo_g <- data.frame()

profvis::profvis({
  trip_count <- 1
  for (trip_count in 1:10) {
    query_chademo <- paste0(
      'select foo.ratios from (select distinct st_linelocatepoint(line, pta) as ratios
      from
      (SELECT
      sp_od(',
      all_trips_nz_df$origin[trip_count],
      ',',
      all_trips_nz_df$destination[trip_count],
      ') as line,
      ST_SetSRID(ST_MakePoint("Longitude", "Latitude" ), 4326) as pta
      from evse_dcfc where ("EV_Connector_Code" = 1 OR "EV_Connector_Code" = 3) AND st_dwithin(ST_GeogFromWKB(ST_SetSRID(ST_MakePoint("Longitude", "Latitude" ), 4326)), ST_GeogFromWKB((select sp_od(',
      all_trips_nz_df$origin[trip_count],
      ',',
      all_trips_nz_df$destination[trip_count],
      '))), cast(',
      LOOKUP_DISTANCE,
      '/0.000621371 as float))) as data ORDER BY ratios ASC) as foo'
      )
    
    res_chademo <-
      DBI::dbSendQuery(con, query_chademo)
    rat_chademo <- DBI::dbFetch(res_chademo)
    DBI::dbClearResult(res_chademo)
    
    rat_chademo <-
      dplyr::as_tibble(rat_chademo) %>% mutate(diff_chademo = ratios - lag(ratios))
    
    
    max_index <- which.max(rat_chademo$diff_chademo)
    if (max_index > 0) {
      ind_maxes <- c(max_index - 1, max_index)
      
      query_chademo <- paste0(
        'select ST_Length(ST_GeographyFromText(subs)), subs
        from (select st_astext(st_LineSubstring(line, pta, ptb)) as subs
        from (select sp_od(',
        all_trips_nz_df$origin[trip_count],
        ',',
        all_trips_nz_df$destination[trip_count],
        ') as line,',
        rat_chademo$ratios[ind_maxes[1]],
        ' as pta, ',
        rat_chademo$ratios[ind_maxes[2]],
        '
        as ptb)
        as data)
        as foo'
        )
    }
    
    res_chademo <-
      DBI::dbSendQuery(con, query_chademo)
    cd_chademo_g <- rbind(cd_chademo_g, DBI::dbFetch(res_chademo))
    
    DBI::dbClearResult(res_chademo)
    }
  })

library(sf)
library(leaflet)
library(rgdal)
library(sp)
sf_1 <- st_as_sfc(cd_chademo_g$subs[1])

st_1 <- sp::spTransform(sf_1, CRS("+proj=longlat +datum=WGS84"))
leaflet() %>% addTiles() %>% addPolylines(data = sf_2) %>% addPolylines(data = sf_1) %>% addPolylines(data = sf_3)

query_sub_chademo <- paste0("select st_intersects(sp_od(98001, 98002)::geometry, st_geomfromtext('", cd_chademo_g$subs[1],  "', 4326))")
res_chademo <-
  DBI::dbSendQuery(con, query_sub_chademo)
sub_cha <- DBI::dbFetch(res_chademo)
DBI::dbClearResult(res_chademo)

sp_query <- 'select st_astext(sp_od(98001, 98002))'
res_chademo <-
  DBI::dbSendQuery(con, sp_query)
sp_1 <- DBI::dbFetch(res_chademo)
DBI::dbClearResult(res_chademo)

sp_sf <- st_as_sfc(sp_1$st_astext) 
leaflet() %>% addTiles() %>%  addPolylines(data = sp_sf, color = "#D5696F", weight = 30, opacity = 1 ) %>% addPolylines(data = sf_1, color = "#5569AF", weight = 10)  %>% addPolylines(data = sf_close)

close_query <- 'select * from line'
res_chademo <-
  DBI::dbSendQuery(con, close_query)
p_close <- DBI::dbFetch(res_chademo)
DBI::dbClearResult(res_chademo)

sf_close <- st_as_sfc(p_close$st_astext)
