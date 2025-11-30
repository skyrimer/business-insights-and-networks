library(tidyverse)
library(arrow)
library(countrycode)
library(fs)

# --- Configuration ---
path <- "./raw_downloads/patents_view/"
files <- dir_ls(path, glob = "*.csv")

# --- Load Files ---
# equivalent to the loop + pd.read_csv + pd.concat
# map_dfr iterates through files and row-binds them
message(paste("Loading", length(files), "files..."))

df <- map_dfr(files, function(f) {
  read_csv(f,
           col_types = cols(.default = "c"), # Read all as character first (safer for mixed types)
           show_col_types = FALSE)
}, .progress = TRUE) # Adds a progress bar (like tqdm)

message(paste("Total rows:", nrow(df)))

# --- Data Cleaning ---
# Drop columns and filter nulls
df <- df %>%
  select(-any_of(c("d_inventor", "d_cpc", "wipo_field_ids", "n_wipo"))) %>%
  filter(!is.na(application_number) & !is.na(application_year))

# Handle n_cpc and n_ipc (Fill NA with 0, convert to int, filter < 50)
df <- df %>%
  mutate(
    n_cpc = as.integer(replace_na(as.numeric(n_cpc), 0)),
    n_ipc = as.integer(replace_na(as.numeric(n_ipc), 0))
  ) %>%
  filter(n_cpc < 50 & n_ipc < 50)

# Integer Conversions
int_cols <- c("assignee_ind", "grant_year", "application_year",
              "inventors", "team_size", "men_inventors", "women_inventors")

df <- df %>%
  mutate(
    across(all_of(int_cols), as.integer),
    # Assignee sequence: fill -1 then int
    assignee_sequence = as.integer(replace_na(as.numeric(assignee_sequence), -1))
  )

# Boolean Conversions
bool_cols <- c("d_assignee", "d_location", "d_application", "d_ipc", "d_wipo")
df <- df %>%
  mutate(across(all_of(bool_cols), ~ as.logical(as.numeric(.))))

# --- Loop for Inventors (1-10) ---
for (i in 1:10) {
  male_col <- paste0("male_flag", i)
  inv_id_col <- paste0("inventor_id", i)
  inv_name_col <- paste0("inventor_name", i)
  placeholder <- paste0("No inventor of ", i, "th rank")
  # Logic for gender mapping
  # distinct case_match allows mapping 1/0 to strings and handling NA in one go
  df[[male_col]] <- case_match(
    as.numeric(df[[male_col]]),
    1 ~ "Male",
    0 ~ "Female",
    NA ~ "Gender not attributed",
    .default = "Gender not attributed"
  )
  df[[male_col]] <- as.factor(df[[male_col]])
  # Logic for ID/Name filling
  df[[inv_id_col]] <- replace_na(df[[inv_id_col]], placeholder)
  df[[inv_name_col]] <- replace_na(df[[inv_name_col]], placeholder)
}

# --- Categorical Filling ---
cat_fill <- list(
  country = "Unknown country",
  county = "Unknown county",
  state = "Unknown state",
  city = "Unknown city",
  assignee = "Unknown assignee",
  first_wipo_sector_title = "Unknown sector",
  first_wipo_field_title = "Unknown field"
)

# Replace NAs using the list and convert to factor
df <- df %>%
  replace_na(cat_fill) %>%
  mutate(across(names(cat_fill), as.factor))

# --- Country Mapping ---
# Using countrycode package to map ISO2 codes to Names
# Note: we coerce to character first in case it's already a factor
df$country_mapped <- countrycode(as.character(df$country), origin = "iso2c", destination = "country.name")

df <- df %>%
  mutate(
    country = coalesce(country_mapped, "Unknown country"),
    country = as.factor(country)
  ) %>%
  select(-country_mapped)

# --- One-Hot Encoding (IPC/CPC Sections) ---
# This mimics the str.get_dummies(sep=" ") logic
# We add a temporary row id to ensure we can join the dummies back correctly
df <- df %>% mutate(temp_row_id = row_number())

for (col_prefix in c("ipc_", "cpc_")) {
  col_name <- paste0(col_prefix, "sections")
  # 1. Clean string (upper, trim)
  # 2. Separate rows by space (this expands the dataframe temporarily)
  # 3. Pivot wider to create boolean columns
  dummies <- df %>%
    select(temp_row_id, all_of(col_name)) %>%
    mutate(!!col_name := str_to_upper(str_trim(replace_na(!!sym(col_name), "")))) %>%
    separate_rows(!!sym(col_name), sep = " ") %>%
    filter(!!sym(col_name) != "") %>% # Remove empty strings resulting from split
    mutate(val = TRUE) %>%
    pivot_wider(
      names_from = !!sym(col_name),
      values_from = val,
      values_fill = FALSE,
      names_prefix = col_prefix
    )
  # Join dummies back to main dataframe
  df <- df %>% left_join(dummies, by = "temp_row_id")
  # Replace NAs introduced by join (where a row had no sections) with FALSE
  new_cols <- setdiff(names(dummies), "temp_row_id")
  df <- df %>%
    mutate(across(all_of(new_cols), ~replace_na(., FALSE))) %>%
    select(-all_of(col_name)) # Drop original section column
}

df <- df %>% select(-temp_row_id)

# --- Output ---
write_parquet(df, "./data/patents.parquet", compression = "gzip")
message("Done.")