# rm(list = ls())
library(doParallel)
library(DBI)
library(RPostgres)

# Find the number of cores in the system
ncores <- parallel::detectCores()
# Make a cluster and define and file to redirect stdout etc. from workers
cl <-
  parallel::makeCluster(ncores, outfile = paste0("log_", format(Sys.time(), "%Y-%m-%d-%H-%M-%S"), ".txt"))

# Specify the libraries needed by workers
parallel::clusterEvalQ(cl, {
  library(DBI)
  library(RPostgres)
  library(glue)
  
  con <-
    DBI::dbConnect(
      RPostgres::Postgres(),
      user = "postgres",
      password = Sys.getenv("PG_PWD"),
      host = "127.0.0.1",
      port = 5432,
      dbname = "wsdot_evse_sp"
    )
})
# Register the cluster
doParallel::registerDoParallel(cl)

LOOKUP_DISTANCE <- 10

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
# evse_dcfc <- tibble::add_column(evse_dcfc, )

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

con <-
  DBI::dbConnect(
    RPostgres::Postgres(),
    user = "postgres",
    password = Sys.getenv("PG_PWD"),
    host = "127.0.0.1",
    port = 5432,
    dbname = "wsdot_evse_sp"
  )

DBI::dbWriteTable(
  con,
  "evse_dcfc",
  evse_dcfc,
  row.names = FALSE,
  append = FALSE,
  overwrite = TRUE
)

DBI::dbGetQuery(con,
                'ALTER TABLE evse_dcfc add column geom geometry(Point, 4326);')
DBI::dbGetQuery(
  con,
  'UPDATE evse_dcfc set geom = st_SetSrid(st_MakePoint("Longitude", "Latitude"), 4326);'
)
DBI::dbGetQuery(con,
                'CREATE INDEX sidx_geom__evse_dcfc ON evse_dcfc using GIST(geom);')

all_trips_nz_df <-
  vroom::vroom("data-raw/all_trips_non_zero.csv", delim = ",")

all_trips_nz_df <-
  tibble::add_column(all_trips_nz_df, cd_chademo = 0, cd_combo = 0)

count <- 100

# Specify the variables that the workers need access to
parallel::clusterExport(
  cl,
  c(
    'query_chademo',
    'query_combo',
    'all_trips_nz_df',
    'evse_dcfc',
    'LOOKUP_DISTANCE'
  )
)
profvis::profvis({
  par_result2 <-
    foreach(j = 1:count,
            .inorder = FALSE,
            .noexport = "con") %dopar% {
              print(paste(j, "of", count))
              
              orig <- all_trips_nz_df$origin[j]
              dest <- all_trips_nz_df$destination[j]
              
              query_chademo <-
                glue::glue(
                  'select st_length(ST_LineSubstring(sp_od({orig},{dest}), 0, max(deltas))::geography) from
                  (select foo.ratios - lag(foo.ratios) over() as deltas from
                  (select distinct st_linelocatepoint(line, pta) as ratios from
                  (SELECT sp_od({orig},{dest}) as line, geom as pta from
                  evse_dcfc where ("EV_Connector_Code" = 1 OR "EV_Connector_Code" = 3) AND st_dwithin(ST_GeogFromWKB(geom), ST_GeogFromWKB((select sp_od({orig},{dest}))), cast({LOOKUP_DISTANCE}/0.000621371 as float))) as data ORDER BY ratios ASC) as foo) as foor;
                  '
                )
              
              sq_chademo <- DBI::dbSendQuery(con, query_chademo)
              res_chademo <- DBI::dbFetch(sq_chademo)
              DBI::dbClearResult(sq_chademo)
              
              c(orig, dest, res_chademo$st_length)
            }
})

stopCluster(cl)
stopImplicitCluster()
