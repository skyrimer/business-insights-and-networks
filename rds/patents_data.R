library(tidyverse)
library(countrycode)
library(fs)
library(config)

cfg <- config::get()
path <- cfg$patents_raw_dir
files <- dir_ls(path, glob = "*.csv")

message(paste("Loading", length(files), "files..."))

df <- map_dfr(files, function(f) {
  read_csv(f,
    col_types = cols(.default = "c"),
    show_col_types = FALSE
  )
}, .progress = TRUE)

message(paste("Total raw rows:", nrow(df)))

# Drop unused columns and filter nulls
df <- df %>%
  select(
    patent_number, application_number, application_year, grant_year,
    assignee, country, n_ipc, n_cpc, team_size,
    ipc_sections
  ) %>%
  filter(!is.na(application_number) & !is.na(application_year))

# Handle n_cpc and n_ipc (Fill NA with 0, convert to int, filter < 50)
df <- df %>%
  mutate(
    n_cpc = as.integer(replace_na(as.numeric(n_cpc), 0)),
    n_ipc = as.integer(replace_na(as.numeric(n_ipc), 0)),
    grant_year = as.integer(grant_year),
    application_year = as.integer(application_year),
    team_size = as.integer(replace_na(as.numeric(team_size), 0))
  ) %>%
  filter(n_cpc < 50 & n_ipc < 50)

# Filter unknown assignees
df <- df %>%
  mutate(assignee = replace_na(assignee, "Unknown assignee")) %>%
  filter(assignee != "Unknown assignee")

df$country_mapped <- countrycode(df$country, origin = "iso2c", destination = "country.name")
df <- df %>%
  mutate(country = coalesce(country_mapped, "Unknown country")) %>%
  select(-country_mapped)

df <- df %>% mutate(temp_row_id = row_number())

dummies <- df %>%
  select(temp_row_id, ipc_sections) %>%
  mutate(ipc_sections = str_to_upper(str_trim(replace_na(ipc_sections, "")))) %>%
  separate_rows(ipc_sections, sep = " ") %>%
  filter(ipc_sections != "") %>%
  mutate(val = 1) %>%
  pivot_wider(
    names_from = ipc_sections,
    values_from = val,
    values_fill = 0,
    names_prefix = "ipc_"
  )

df <- df %>%
  left_join(dummies, by = "temp_row_id") %>%
  select(-temp_row_id, -ipc_sections)

# Ensure all IPC columns exist (A-H)
ipc_section_cols <- c(
  "ipc_A", "ipc_B", "ipc_C", "ipc_D",
  "ipc_E", "ipc_F", "ipc_G", "ipc_H"
)
for (col in ipc_section_cols) {
  if (!col %in% names(df)) {
    df[[col]] <- 0
  }
}
df <- df %>%
  mutate(across(all_of(ipc_section_cols), ~ replace_na(., 0)))

message(paste("Cleaned rows:", nrow(df)))

message("Aggregating data to Assignee level...")

firm_df <- df %>%
  group_by(assignee) %>%
  summarise(
    total_applications = n_distinct(application_number),
    granted_patents = sum(!is.na(grant_year) & grant_year > 0),
    avg_ipc_scope = mean(n_ipc, na.rm = TRUE),
    avg_team_size = mean(team_size, na.rm = TRUE),
    sum_ipc_A = sum(ipc_A, na.rm = TRUE),
    sum_ipc_B = sum(ipc_B, na.rm = TRUE),
    sum_ipc_C = sum(ipc_C, na.rm = TRUE),
    sum_ipc_D = sum(ipc_D, na.rm = TRUE),
    sum_ipc_E = sum(ipc_E, na.rm = TRUE),
    sum_ipc_F = sum(ipc_F, na.rm = TRUE),
    sum_ipc_G = sum(ipc_G, na.rm = TRUE),
    sum_ipc_H = sum(ipc_H, na.rm = TRUE),
    .groups = "drop"
  )

firm_df <- firm_df %>%
  mutate(
    grant_success_rate = ifelse(total_applications > 0, granted_patents / total_applications, 0),
    total_ipc_mentions = sum_ipc_A + sum_ipc_B + sum_ipc_C + sum_ipc_D +
      sum_ipc_E + sum_ipc_F + sum_ipc_G + sum_ipc_H,
    share_tech_G = ifelse(total_ipc_mentions > 0, sum_ipc_G / total_ipc_mentions, 0)
  )

firm_df <- firm_df %>%
  mutate(
    share_A = ifelse(total_ipc_mentions > 0, sum_ipc_A / total_ipc_mentions, 0),
    share_B = ifelse(total_ipc_mentions > 0, sum_ipc_B / total_ipc_mentions, 0),
    share_C = ifelse(total_ipc_mentions > 0, sum_ipc_C / total_ipc_mentions, 0),
    share_D = ifelse(total_ipc_mentions > 0, sum_ipc_D / total_ipc_mentions, 0),
    share_E = ifelse(total_ipc_mentions > 0, sum_ipc_E / total_ipc_mentions, 0),
    share_F = ifelse(total_ipc_mentions > 0, sum_ipc_F / total_ipc_mentions, 0),
    share_G = ifelse(total_ipc_mentions > 0, sum_ipc_G / total_ipc_mentions, 0),
    share_H = ifelse(total_ipc_mentions > 0, sum_ipc_H / total_ipc_mentions, 0),
    tech_specialist_hhi = share_A^2 + share_B^2 + share_C^2 + share_D^2 +
      share_E^2 + share_F^2 + share_G^2 + share_H^2
  ) %>%
  select(-starts_with("share_"), -starts_with("sum_ipc_"))

message(paste("Saving processed data for", nrow(firm_df), "unique assignees."))
write.csv(firm_df, cfg$patents_processed_file, row.names = FALSE)
