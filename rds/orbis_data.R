library(dplyr)
library(readxl)
library(config)

if (!requireNamespace("nstandr", quietly = TRUE)) {
  if (!requireNamespace("devtools", quietly = TRUE)) install.packages("devtools")
  devtools::install_github("stasvlasov/nstandr")
}
library(nstandr)
source("rds/reporting_utils.R")


cfg <- config::get()
print_section("Loading Orbis data...")

message("Loading Orbis data...")

# Read the Excel file
orbis_raw <- read_excel(
  cfg$orbis_raw_file,
  sheet = "Results",
  .name_repair = "unique"
)

message("Raw columns found: ", ncol(orbis_raw))
message("Column names: ")
print(names(orbis_raw))


# Create a function to normalize column names
normalize_col_name <- function(x) {
  x <- tolower(x)
  x <- gsub("\\s+", "_", x)
  x <- gsub("[^a-z0-9_]", "", x)
  x <- gsub("_+", "_", x)
  x <- gsub("^_|_$", "", x)
  x
}

# Normalize all column names
names(orbis_raw) <- normalize_col_name(names(orbis_raw))

message("\nNormalized column names:")
print(names(orbis_raw))


# Find columns by pattern matching
find_col <- function(df, patterns) {
  for (p in patterns) {
    matches <- grep(p, names(df), value = TRUE, ignore.case = TRUE)
    if (length(matches) > 0) {
      return(matches[1])
    }
  }
  return(NA_character_)
}

# Identify key columns based on actual Orbis export format
col_mapping <- list(
  company_name = find_col(orbis_raw, c("company_name_latin_alphabet", "company_name", "name_latin")),
  country_iso = find_col(orbis_raw, c("country_iso_code", "country_iso")),
  city = find_col(orbis_raw, c("city_latin_alphabet", "city_latin", "city")),
  nace_code = find_col(orbis_raw, c("nace_rev_2_core_code_4_digits", "nace.*core.*code", "nace.*4.*digit")),
  consolidation_code = find_col(orbis_raw, c("consolidation_code", "consolidation")),
  last_avail_year = find_col(orbis_raw, c("last_avail_year", "last_available_year")),
  address_type = find_col(orbis_raw, c("^type$")),
  peer_group_size = find_col(orbis_raw, c("peer_group_size")),
  peer_group_name = find_col(orbis_raw, c("peer_group_name")),
  main_customers = find_col(orbis_raw, c("main_customers")),
  main_distribution_sites = find_col(orbis_raw, c("main_distribution_sites")),
  information_provider = find_col(orbis_raw, c("information_provider")),
  size_classification = find_col(orbis_raw, c("size_classification")),
  employees_year = find_col(orbis_raw, c("number_of_employees_last_avail_yr")),
  employees = find_col(orbis_raw, c("number_of_employees_last_avail_value", "number_of_employees")),
  total_assets_year = find_col(orbis_raw, c("total_assets_th_eur_last_avail_yr")),
  total_assets_eur = find_col(orbis_raw, c("total_assets_th_eur_last_avail_value")),
  total_assets_usd = find_col(orbis_raw, c("total_assets_th_usd_last_avail_value")),
  companies_in_group = find_col(orbis_raw, c("no_of_companies_in_corporate_group", "companies.*corporate.*group")),
  entity_type = find_col(orbis_raw, c("entity_type")),
  shareholders = find_col(orbis_raw, c("no_of_shareholders", "shareholders")),
  subsidiaries = find_col(orbis_raw, c("no_of_subsidiaries", "subsidiaries")),
  roa = find_col(orbis_raw, c("roa_using_net_income_last_avail_yr", "roa")),
  rd_ratio = find_col(orbis_raw, c("rd_expenses_operating_revenue_last_avail_yr", "rd_expenses")),
  export_ratio = find_col(orbis_raw, c("export_revenue_operating_revenue_last_avail_yr")),
  export_revenue = find_col(orbis_raw, c("export_revenue_th_eur_last_avail_yr")),
  long_term_debt = find_col(orbis_raw, c("long_term_debt_th_eur_last_avail_yr", "long_term_debt"))
)

message("\nColumn mapping results:")
for (name in names(col_mapping)) {
  message("  ", name, " -> ", ifelse(is.na(col_mapping[[name]]), "NOT FOUND", col_mapping[[name]]))
}

# Build select expression for available columns
available_cols <- col_mapping[!is.na(col_mapping)]

if (length(available_cols) == 0) {
  stop("No expected columns found in ORBIS data!")
}

# Select and rename columns
orbis_data <- orbis_raw

# Build rename vector dynamically
rename_vec <- setNames(unlist(available_cols), names(available_cols))
orbis_data <- orbis_data %>% rename(!!!rename_vec[rename_vec %in% names(orbis_data)])

message("\nConverting data types...")

# Define which columns should be numeric
numeric_cols <- c(
  "employees", "total_assets_eur", "total_assets_usd",
  "shareholders", "subsidiaries", "companies_in_group",
  "roa", "rd_ratio", "export_ratio", "export_revenue",
  "long_term_debt", "peer_group_size"
)

# Convert "n.a." and similar to NA, then to numeric
for (col in numeric_cols) {
  if (col %in% names(orbis_data)) {
    orbis_data[[col]] <- orbis_data[[col]] %>%
      as.character() %>%
      na_if("n.a.") %>%
      na_if("n.s.") %>%
      na_if("-") %>%
      na_if("") %>%
      as.numeric()
  }
}

# Rename to final standardized names
orbis_data <- orbis_data %>%
  rename(
    employees_last_value = any_of("employees"),
    n_shareholders = any_of("shareholders"),
    n_subsidiaries = any_of("subsidiaries"),
    n_companies_group = any_of("companies_in_group")
  )

# Create entity type feature if available
if ("entity_type" %in% names(orbis_data)) {
  orbis_data <- orbis_data %>%
    mutate(
      entity_type = factor(entity_type),
      is_guo = as.integer(grepl("GUO", entity_type, ignore.case = TRUE)),
      is_controlled_sub = as.integer(grepl("Controlled", entity_type, ignore.case = TRUE))
    )
} else {
  orbis_data$is_guo <- NA_integer_
  orbis_data$is_controlled_sub <- NA_integer_
}

# Create size classification factor if available
if ("size_classification" %in% names(orbis_data)) {
  orbis_data$size_classification <- factor(orbis_data$size_classification)
}

message("Orbis companies loaded: ", nrow(orbis_data))

message("\n--- Missing Values Report ---")

key_vars <- c(
  "company_name", "country_iso", "employees_last_value", "total_assets_eur",
  "n_shareholders", "n_subsidiaries", "roa", "rd_ratio"
)

for (col in key_vars) {
  if (col %in% names(orbis_data)) {
    n_miss <- sum(is.na(orbis_data[[col]]))
    pct_miss <- round(n_miss / nrow(orbis_data) * 100, 1)
    message("  ", col, ": ", n_miss, " missing (", pct_miss, "%)")
  } else {
    message("  ", col, ": COLUMN NOT FOUND")
  }
}

message("\nStandardizing company names...")

if ("company_name" %in% names(orbis_data)) {
  orbis_data$company_name_std <- orbis_data$company_name %>%
    as.character() %>%
    trimws() %>%
    standardize_magerman()
} else {
  stop("company_name column not found - cannot proceed!")
}

message("\nApplying SDC-ORBIS mapping to rename companies...")

sdc_orbis_map <- read.csv(cfg$sdc_orbis_map_file, stringsAsFactors = FALSE)

# Clean the mapping and remove country suffix from SDC_data (after last comma)
sdc_orbis_map <- sdc_orbis_map %>%
  mutate(
    SDC_data = trimws(SDC_data),
    ORBIS_data = trimws(ORBIS_data),
    SDC_data_clean = sub(",\\s*[^,]*$", "", SDC_data)
  ) %>%
  filter(!is.na(ORBIS_data) & ORBIS_data != "")

message("Mapping entries: ", nrow(sdc_orbis_map))

# Create a lookup: ORBIS_data -> SDC_data_clean (without country)
orbis_to_sdc <- setNames(sdc_orbis_map$SDC_data_clean, sdc_orbis_map$ORBIS_data)

# Rename company_name in orbis_data using the mapping
orbis_data <- orbis_data %>%
  mutate(
    company_name_original = company_name, # Keep original for reference
    company_name = ifelse(company_name %in% names(orbis_to_sdc),
      orbis_to_sdc[company_name],
      company_name
    )
  )

renamed_count <- sum(orbis_data$company_name != orbis_data$company_name_original)
message("Companies renamed using mapping: ", renamed_count, " / ", nrow(orbis_data))

message("\n--- Final Dataset Summary ---")
message("Total rows: ", nrow(orbis_data))
message("Total columns: ", ncol(orbis_data))
message("Columns: ", paste(names(orbis_data), collapse = ", "))

write.csv(orbis_data, cfg$orbis_cleaned_file, row.names = FALSE)
message("\nSaved to ", cfg$orbis_cleaned_file)
message("Done.")
