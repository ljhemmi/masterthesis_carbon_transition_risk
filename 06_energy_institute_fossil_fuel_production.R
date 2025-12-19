rm(list = ls()); gc()

# all packages that are needed
packages <- c(
  "tidyr",
  "dplyr",
  "writexl",
  "readxl",
  "data.table",
  "bit64",
  "arrow",
  "lubridate",
  "readr",
  "countrycode"
)

# install missing packages
installed <- packages %in% installed.packages()[, "Package"]
if (any(!installed)) {
  install.packages(packages[!installed])
}

# load all the packages
lapply(packages, library, character.only = TRUE)

# =============================================================================.
# Setup ----
# =============================================================================.

final_locs <- c(
  "USA","CAN","NLD","ISR","BMU","GBR","JPN","IRL","CHN","SWE","MYS","DNK","CHE","ZMB","NOR","NZL","AUS","FRA","ESP","LUX","ARE","ITA",
  "DEU","HKG","SGP","ZAF","BEL","FIN","LIE","BRA","AUT","PRT","VGB","MEX","KOR","THA","IND","IMN","ARG","CHL","CYM","IDN","GRC","RUS",
  "TWN","BHS","CRI","GGY","URY","JEY","COL","PER","PNG","TUR","POL","BLZ","QAT","MHL","PAN","CYP","MCO","NGA","CZE","HUN","MAR","SVN",
  "MUS","PHL","ISL","KWT","ROU","SAU","MLT","CMR","GIB","LTU","BHR","TGO","KHM","EST","BGR","SLB","SVK","KGZ","UKR","MAC","MNG","KAZ",
  "VNM","VIR"
)

# =============================================================================.
# Data Load EI ----
# =============================================================================.

# oil ----
# spot crude oil prices per barell
spot_crude_prices <- read_excel("../01_data/08_energy institute/EI-Stats-Review-ALL-data.xlsx", 
           sheet = "Spot crude prices",
           range = "A5:E57",
           col_names = c("year","Dubai","Brent","Nigerian_Forcados","West_Texas_Intermed"))

setDT(spot_crude_prices)
spot_crude_prices[, year := as.integer(year)]
spot_crude_prices[, Brent := as.numeric(Brent)]
spot_crude_prices <- spot_crude_prices[, .(year, Brent)]

# load oil production in barrels
oil_prod_barrels <- read_excel("../01_data/08_energy institute/EI-Stats-Review-ALL-data.xlsx", 
                                sheet = "Oil Production - barrels",
                                range = "A3:BI81"
                                )

setDT(oil_prod_barrels)
setnames(oil_prod_barrels, old = names(oil_prod_barrels)[1], new = "country")

oil_prod_barrels <- oil_prod_barrels %>%
  filter(!if_all(everything(), is.na))

# melt to long format
oil_prod_barrels <- melt(
  oil_prod_barrels,
  id.vars        = "country",
  variable.name  = "year",
  value.name     = "thousand_barrels_daily",
  variable.factor = FALSE
)

oil_prod_barrels[, yearly_barrels := thousand_barrels_daily*1000*365]

# make year numeric (optional but usually useful)
oil_prod_barrels[, year := as.integer(year)]


setorder(oil_prod_barrels,country, year)

# join brent prices
oil_prod_barrels <- spot_crude_prices[oil_prod_barrels, on = .(year)]
oil_prod_barrels <- oil_prod_barrels[year>=2010]

# calculate total USD production
oil_prod_barrels[, total_oil_prod_USD := Brent*yearly_barrels]


# gas ----
# gas prices USD per million Btu
gas_prices_USD <- read_excel("../01_data/08_energy institute/EI-Stats-Review-ALL-data.xlsx", 
                                sheet = "Gas Prices ",
                                range = "A7:K47",
                                col_names = c("year","Japan","South_Korea","Platts_JKM","China_Mainland","Platts_WIM","Euro_Platts_NWE","Zeebrugge","UK_NBP","Netherlands_TTF","US_Henry_Hub"))


setDT(gas_prices_USD)

gas_prices_USD[, year := as.integer(year)]
gas_prices_USD[, Netherlands_TTF_USD_per_m_Btu := as.numeric(Netherlands_TTF)]
gas_prices_USD <- gas_prices_USD[, .(year, Netherlands_TTF_USD_per_m_Btu)]

# gas production
gas_prod_bn_cub_m <- read_excel("../01_data/08_energy institute/EI-Stats-Review-ALL-data.xlsx", 
                             sheet = "Gas Production - Bcm",
                             range = "A3:BD75")

setDT(gas_prod_bn_cub_m)
setnames(gas_prod_bn_cub_m, old = names(gas_prod_bn_cub_m)[1], new = "country")

gas_prod_bn_cub_m <- gas_prod_bn_cub_m %>%
  filter(!if_all(everything(), is.na))

# melt to long format
gas_prod_bn_cub_m <- melt(
  gas_prod_bn_cub_m,
  id.vars        = "country",
  variable.name  = "year",
  value.name     = "nat_gas_bcm",
  variable.factor = FALSE
)


setorder(gas_prod_bn_cub_m, country, year)

gas_prod_bn_cub_m[, year := as.integer(year)]

# join gas prices
gas_prod_bn_cub_m <- gas_prices_USD[gas_prod_bn_cub_m, on = .(year)]
gas_prod_bn_cub_m <- gas_prod_bn_cub_m[year >= 2010]

conversion_bcm_to_trBtu <- 34.1214163312809

gas_prod_bn_cub_m[, gas_prod_m_Btu := conversion_bcm_to_trBtu*(10^6)*nat_gas_bcm]
gas_prod_bn_cub_m[, total_natgas_prod_USD := gas_prod_m_Btu*Netherlands_TTF_USD_per_m_Btu]


# coal ----
# spot coal prices
coal_prices_USD_per_Tonne <- read_excel("../01_data/08_energy institute/EI-Stats-Review-ALL-data.xlsx", 
                                sheet = "Coal & Uranium - Prices",
                                range = "A5:I42",
                                col_names = c("year","US","Colombia","Northwest_Europe","South_Africa","Indonesia","South_China","Japan","Australia"))

setDT(coal_prices_USD_per_Tonne)
coal_prices_USD_per_Tonne[, year := as.integer(year)]
coal_prices_USD_per_Tonne[, Northwest_Europe_USD_per_tonne := as.numeric(Northwest_Europe)]
coal_prices_USD_per_Tonne <- coal_prices_USD_per_Tonne[, .(year, Northwest_Europe_USD_per_tonne)]

# load 
coal_prod_m_Tonnes <- read_excel("../01_data/08_energy institute/EI-Stats-Review-ALL-data.xlsx", 
                                 sheet = "Coal Production - mt",
                                 range = "A3:AS59")

setDT(coal_prod_m_Tonnes)
setnames(coal_prod_m_Tonnes, old = names(coal_prod_m_Tonnes)[1], new = "country")

coal_prod_m_Tonnes <- coal_prod_m_Tonnes %>%
  filter(!if_all(everything(), is.na))

# melt to long format
coal_prod_m_Tonnes <- melt(
  coal_prod_m_Tonnes,
  id.vars        = "country",
  variable.name  = "year",
  value.name     = "coal_m_tonnes",
  variable.factor = FALSE
)


setorder(coal_prod_m_Tonnes, country, year)

coal_prod_m_Tonnes[, year := as.integer(year)]

# join coal prices
coal_prod_m_Tonnes <- coal_prices_USD_per_Tonne[coal_prod_m_Tonnes, on = .(year)]
coal_prod_m_Tonnes <- coal_prod_m_Tonnes[year >= 2010]

coal_prod_m_Tonnes[, total_coal_prod_USD := coal_m_tonnes*(10^6)*Northwest_Europe_USD_per_tonne]


# combined file:
nrow(oil_prod_barrels[, .(year, country, total_oil_prod_USD)])
nrow(gas_prod_bn_cub_m[, .(year, country, total_natgas_prod_USD)])
nrow(coal_prod_m_Tonnes[, .(year, country, total_coal_prod_USD)])

oil_dt  <- oil_prod_barrels[, .(year, country, total_oil_prod_USD)]
gas_dt  <- gas_prod_bn_cub_m[, .(year, country, total_natgas_prod_USD)]
coal_dt <- coal_prod_m_Tonnes[, .(year, country, total_coal_prod_USD)]

# 1) Join gas onto oil (oil is the "x" table, i.e. base)
energy_panel <- Reduce(
  function(x, y) merge(x, y, by = c("year", "country"), all = TRUE),
  list(oil_dt, gas_dt, coal_dt)
)

setorder(energy_panel, country, year)
# 3) Order columns: year, country, oil, gas, coal
setcolorder(
  energy_panel,
  c("year", "country",
    "total_oil_prod_USD",
    "total_natgas_prod_USD",
    "total_coal_prod_USD")
)

# optional sanity checks
nrow(oil_dt)          # base row count
nrow(energy_panel)    # should match oil_dt row count

energy_panel[, total_ff_prod_USD := rowSums(.SD, na.rm = TRUE),
             .SDcols = c("total_oil_prod_USD",
                         "total_natgas_prod_USD",
                         "total_coal_prod_USD")]


countries <- unique(energy_panel$country)

iso_map <- data.table(
  country = countries,
  iso3_country = countrycode(
    countries,
    origin      = "country.name",
    destination = "iso3c"
))

# keep only actual countries (drop aggregates etc.)
iso_map <- iso_map[!is.na(iso3_country)]

energy_panel <- iso_map[energy_panel, on = .(country)]

energy_panel <- energy_panel[country != "USSR"]

# build a yearly price table from the series you already used
prices_dt <- Reduce(
  function(x, y) merge(x, y, by = "year", all = TRUE),
  list(
    spot_crude_prices[, .(year, Brent)],
    gas_prices_USD[, .(year, Netherlands_TTF_USD_per_m_Btu)],
    coal_prices_USD_per_Tonne[, .(year, Northwest_Europe_USD_per_tonne)]
  )
)

# keep only years that appear in the panel (optional)
prices_dt <- prices_dt[year %in% unique(energy_panel$year)]

# (optional but clearer) rename price columns
setnames(
  prices_dt,
  old = c("Brent", "Netherlands_TTF_USD_per_m_Btu", "Northwest_Europe_USD_per_tonne"),
  new = c("price_oil_Brent_USD_per_bbl",
          "price_gas_TTF_USD_per_mBtu",
          "price_coal_NWE_USD_per_tonne")
)

# merge prices onto the panel by year (replicates prices for all countries in that year)
energy_panel <- merge(
  energy_panel,
  prices_dt,
  by    = "year",
  all.x = TRUE,
  sort  = FALSE
)

# reorder columns: year, country, iso, prices, then totals
setcolorder(
  energy_panel,
  c("year", "country", "iso3_country",
    "price_oil_Brent_USD_per_bbl",
    "price_gas_TTF_USD_per_mBtu",
    "price_coal_NWE_USD_per_tonne",
    "total_oil_prod_USD",
    "total_natgas_prod_USD",
    "total_coal_prod_USD",
    "total_ff_prod_USD")
)

# Target years
years_target <- 2010:2024

# 1) Subset to desired ISO codes and years
energy_panel_sub <- energy_panel[
  iso3_country %in% final_locs & year %in% years_target
]

# 2) Find which locations in final_locs are completely missing
missing_locs <- setdiff(final_locs, unique(energy_panel_sub$iso3_country))

# 3) For missing locations, create rows for all years 2010–2024
if (length(missing_locs) > 0) {
  # base grid: year × iso3_country
  add_dt <- CJ(
    year         = years_target,
    iso3_country = missing_locs,
    unique       = TRUE
  )
  
  # 4) Add remaining columns with NA, preserving types from energy_panel_sub
  cols_missing <- setdiff(names(energy_panel_sub), names(add_dt))
  
  for (col in cols_missing) {
    proto <- energy_panel_sub[[col]]
    
    val <- if (is.integer(proto)) {
      NA_integer_
    } else if (is.numeric(proto)) {
      NA_real_
    } else if (is.character(proto)) {
      NA_character_
    } else if (is.logical(proto)) {
      NA
    } else {
      NA
    }
    
    add_dt[, (col) := val]
  }
  
  # 5) Bind back to the subset
  energy_panel_sub <- rbind(energy_panel_sub, add_dt, fill = TRUE)
}

# 6) Order nicely
setorder(energy_panel_sub, iso3_country, year)

energy_panel <- energy_panel_sub

write_parquet(energy_panel,"../01_data/00_R_outputs/energy_institute_total_ff_USD.parquet")

# =============================================================================.
# Data Load EIA ----
# =============================================================================.
# load oil ----
EIA_oil <- read_csv("../01_data/10_EIA/EIA_oil_2024.csv",
         skip = 1)
setDT(EIA_oil)
EIA_oil <- EIA_oil[-1,-1]
# Rename first column to "country"
setnames(EIA_oil, old = names(EIA_oil)[1], new = "country")

# Melt all remaining columns into year/value pairs
EIA_oil <- melt(
  EIA_oil,
  id.vars = "country",
  variable.name = "year",
  value.name = "oil_prod_t_barrels_per_day"
)

EIA_oil[
  , `:=`(
    year = as.integer(as.character(year)),
    oil_prod_t_barrels_per_day = as.numeric(oil_prod_t_barrels_per_day)
  )
]

EIA_oil[
  ,
  oil_prod_barrels_tot := oil_prod_t_barrels_per_day * 1000 * 365
]

EIA_oil <- EIA_oil[year>=2007]

# join oil prices (Brent)
EIA_oil <- spot_crude_prices[EIA_oil, on = .(year)]

EIA_oil[, total_oil_prod_USD := Brent*oil_prod_barrels_tot]



# load gas ----
EIA_gas <- read_csv("../01_data/10_EIA/EIA_natural_gas_2023.csv",
                    skip = 1)
setDT(EIA_gas)
EIA_gas <- EIA_gas[-1,-1]
# Rename first column to "country"
setnames(EIA_gas, old = names(EIA_gas)[1], new = "country")

EIA_gas <- melt(
  EIA_gas,
  id.vars = "country",
  variable.name = "year",
  value.name = "gas_prod_bcm"
)

EIA_gas[
  , `:=`(
    year = as.integer(as.character(year)),
    gas_prod_bcm = as.numeric(gas_prod_bcm)
  )
]

EIA_gas <- EIA_gas[year>=2007]

# join gas prices
EIA_gas <- gas_prices_USD[EIA_gas, on = .(year)]

conversion_bcm_to_trBtu <- 34.1214163312809

EIA_gas[, gas_prod_m_Btu := conversion_bcm_to_trBtu*(10^6)*gas_prod_bcm]
EIA_gas[, total_natgas_prod_USD := gas_prod_m_Btu*Netherlands_TTF_USD_per_m_Btu]


# load coal ----
EIA_coal <- read_csv("../01_data/10_EIA/EIA_natural_coal_2023.csv",
                    skip = 1)
setDT(EIA_coal)
EIA_coal <- EIA_coal[-1,-1]
# Rename first column to "country"
setnames(EIA_coal, old = names(EIA_coal)[1], new = "country")

EIA_coal <- melt(
  EIA_coal,
  id.vars = "country",
  variable.name = "year",
  value.name = "coal_prod_metric_tons"
)

EIA_coal[
  , `:=`(
    year = as.integer(as.character(year)),
    coal_prod_metric_tons = as.numeric(coal_prod_metric_tons)
  )
]

EIA_coal <- EIA_coal[year>=2007]

EIA_coal[
  ,
  coal_prod_M_metric_tons := coal_prod_metric_tons / 1000
]

EIA_coal <- coal_prices_USD_per_Tonne[EIA_coal, on = .(year)]

EIA_coal[, total_coal_prod_USD := coal_prod_M_metric_tons*(10^6)*Northwest_Europe_USD_per_tonne]



# EIA: combine oil, gas, coal into one panel with total fossil-fuel USD ----
# =============================================================================.

# Keep only the relevant columns for each fuel
EIA_oil_dt  <- EIA_oil[, .(year, country, total_oil_prod_USD)]
EIA_gas_dt  <- EIA_gas[, .(year, country, total_natgas_prod_USD)]
EIA_coal_dt <- EIA_coal[, .(year, country, total_coal_prod_USD)]

# Merge the three fuels into one EIA panel
EIA_energy_panel <- Reduce(
  function(x, y) merge(x, y, by = c("year", "country"), all = TRUE),
  list(EIA_oil_dt, EIA_gas_dt, EIA_coal_dt)
)

setorder(EIA_energy_panel, country, year)

# Order columns: year, country, oil, gas, coal
setcolorder(
  EIA_energy_panel,
  c("year", "country",
    "total_oil_prod_USD",
    "total_natgas_prod_USD",
    "total_coal_prod_USD")
)

# Total fossil fuel production value (EIA)
EIA_energy_panel[
  ,
  total_ff_prod_USD := rowSums(
    .SD,
    na.rm = TRUE
  ),
  .SDcols = c(
    "total_oil_prod_USD",
    "total_natgas_prod_USD",
    "total_coal_prod_USD"
  )
]

# =============================================================================.
# EIA: add ISO codes and prices (reuse same mapping & prices) ----
# =============================================================================.

# Build ISO mapping for EIA countries
countries_EIA <- unique(EIA_energy_panel$country)

iso_map_EIA <- data.table(
  country = countries_EIA,
  iso3_country = countrycode(
    countries_EIA,
    origin      = "country.name",
    destination = "iso3c"
  )
)

# Keep only actual countries
iso_map_EIA <- iso_map_EIA[!is.na(iso3_country)]
iso_map_EIA[iso_map_EIA$iso3_country %in% iso_map_EIA$iso3_country[duplicated(iso_map_EIA$iso3_country)]]

drop_names <- c(
  "Former U.S.S.R.",
  "Germany, West",
  "U.S. Territories"
)

# Drop these from iso_map_EIA
iso_map_EIA_clean <- iso_map_EIA[!country %in% drop_names]

# Join ISO codes
EIA_energy_panel <- iso_map_EIA_clean[EIA_energy_panel, on = .(country)]

# Reuse prices_dt already built above for EI
# If you want to restrict to panel years:
prices_dt_EIA <- prices_dt[year %in% unique(EIA_energy_panel$year)]

EIA_energy_panel <- merge(
  EIA_energy_panel,
  prices_dt_EIA,
  by    = "year",
  all.x = TRUE,
  sort  = FALSE
)

# Reorder columns similarly to EI panel
setcolorder(
  EIA_energy_panel,
  c("year", "country", "iso3_country",
    "price_oil_Brent_USD_per_bbl",
    "price_gas_TTF_USD_per_mBtu",
    "price_coal_NWE_USD_per_tonne",
    "total_oil_prod_USD",
    "total_natgas_prod_USD",
    "total_coal_prod_USD",
    "total_ff_prod_USD")
)

# =============================================================================.
# Restrict EIA panel to final_locs and ensure full 2010–2024 coverage ----
# =============================================================================.

years_target <- 2010:2024

# 1) Subset to desired ISO codes and target years
EIA_energy_panel_sub <- EIA_energy_panel[
  iso3_country %in% final_locs & year %in% years_target
]

# 2) Find which ISO3 codes in final_locs are missing entirely
missing_locs_EIA <- setdiff(final_locs, unique(EIA_energy_panel_sub$iso3_country))

# 3) For missing locations, create rows for all years 2010–2024
if (length(missing_locs_EIA) > 0) {
  # base grid: year × iso3_country
  add_EIA <- CJ(
    year         = years_target,
    iso3_country = missing_locs_EIA,
    unique       = TRUE
  )
  
  # 4) Add remaining columns with NA, preserving types from existing panel
  cols_missing_EIA <- setdiff(names(EIA_energy_panel_sub), names(add_EIA))
  
  for (col in cols_missing_EIA) {
    proto <- EIA_energy_panel_sub[[col]]
    
    val <- if (is.integer(proto)) {
      NA_integer_
    } else if (is.numeric(proto)) {
      NA_real_
    } else if (is.character(proto)) {
      NA_character_
    } else if (is.logical(proto)) {
      NA
    } else {
      NA
    }
    
    add_EIA[, (col) := val]
  }
  
  # 5) Bind back to the subset
  EIA_energy_panel_sub <- rbind(EIA_energy_panel_sub, add_EIA, fill = TRUE)
}

# 6) Order nicely and overwrite original
setorder(EIA_energy_panel_sub, iso3_country, year)
EIA_energy_panel <- EIA_energy_panel_sub


# ensure that negative values are treated as 0
EIA_energy_panel[total_ff_prod_USD < 0, total_ff_prod_USD := 0]


# Optional: write EIA panel to parquet
write_parquet(
  EIA_energy_panel,
  "../01_data/00_R_outputs/EIA_total_ff_USD.parquet"
)


# =============================================================================.
# Final table: EI vs EIA total fossil-fuel USD values ----
# =============================================================================.

# Extract EI total fossil-fuel value by year + iso3
EI_totals <- energy_panel[
  ,
  .(year, iso3_country, country,
    EI_total_ff_USD = total_ff_prod_USD)
]

# Extract EIA total fossil-fuel value by year + iso3
EIA_totals <- EIA_energy_panel[
  ,
  .(year, iso3_country, country,
    EIA_total_ff_USD = total_ff_prod_USD)
]

# Merge into final comparison table
final_ff_values <- merge(
  EI_totals,
  EIA_totals,
  by   = c("year", "iso3_country"),
  all  = TRUE,
  sort = FALSE
)

setorder(final_ff_values, iso3_country, year)


# Choose a single country column (EI if available, else EIA)
final_ff_values[
  ,
  country := fifelse(
    !is.na(country.x),
    country.x,
    country.y
  )
]

# Clean up column order
setcolorder(
  final_ff_values,
  c("year", "country", "iso3_country",
    "EI_total_ff_USD", "EIA_total_ff_USD")
)

final_ff_values[
  ,
  total_ff_USD := fifelse(
    !is.na(EI_total_ff_USD),
    EI_total_ff_USD,
    EIA_total_ff_USD
  )
]

output <- final_ff_values[,.(year,iso3_country,total_ff_USD)]

write_parquet(output, "../01_data/00_R_outputs/EIA_EI_total_ff_prod_USD.parquet")

