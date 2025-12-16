# ============================================================================
# David vs. Goliath: Partner Size Diversity and Innovation (SIC 3571, Asia)
# ============================================================================

# ----------------------------------------
# 1. Setup
# ----------------------------------------
library(tidyr)
library(dplyr)
library(igraph)
library(readxl)
library(purrr)

if (!requireNamespace("arrow", quietly = TRUE)) {
  install.packages("arrow")
}
library(arrow)
if (!requireNamespace("countrycode", quietly = TRUE)) {
  install.packages("countrycode")
}
library(countrycode)

if (!requireNamespace("nstandr", quietly = TRUE)) {
  if (!requireNamespace("devtools", quietly = TRUE)) {
    install.packages("devtools")
  }
  devtools::install_github("stasvlasov/nstandr")
}
library(nstandr)

# Helper to normalize messy Excel headers so we can match them reliably
normalize_orbis_name <- function(x) {
  x <- gsub("\\s+", " ", x)
  x <- trimws(x)
  x <- tolower(x)
  x <- gsub("[^a-z0-9]", "_", x)
  gsub("_+", "_", x)
}

# Apply a name map (new -> normalized old) while ignoring columns that don't exist
apply_orbis_col_map <- function(df, mapping) {
  for (target in names(mapping)) {
    source <- mapping[[target]]
    idx <- match(source, names(df))
    if (!is.na(idx)) {
      names(df)[idx] <- target
    }
  }
  df
}

# ----------------------------------------
# 2. Load and Filter SDC Data
# ----------------------------------------
sdc_data <- readRDS("./data/SDC_data_2021.rds")

# Asia countries list (comprehensive, matching EDA)
asia_countries <- c("Japan","Iran","India","Indonesia","Malaysia","Pakistan","Thailand",
          "Taiwan","Vietnam","China","South Korea","Philippines","Singapore",
          "Uzbekistan","Brunei","Myanmar(Burma)","Laos","Cambodia","Mongolia",
          "Afghanistan","Kazakhstan","North Korea","Bangladesh","Sri Lanka",
          "Armenia","Turkmenistan","Kyrgyzstan","Nepal","Maldives","Bhutan",
          "Timor-Leste","Oman","Yemen","Jordan","Israel","Lebanon","Iraq",
          "Bahrain","Qatar","Utd Arab Em","Palestine","Cyprus","Georgia","Turkey")

SIC_CODE <- 3571

# 1. Filter to strategic alliances (matching EDA approach)
sdc_filt <- sdc_data %>%
  filter(
    status == "Completed/Signed",
    type == "Strategic Alliance",
    date_terminated == "",
    date_announced > "1999-12-31",
    !is.na(participants),
    !is.na(deal_number)
  )

# Disambiguate company names
sdc_filt$participants <- sdc_filt$participants %>%
  trimws() %>%
  standardize_magerman()

# 2. Identify focal firms (Asia + SIC 3571)
focal_firms <- sdc_filt %>%
  filter(
    SIC_primary == SIC_CODE,
    participant_nation %in% asia_countries
  ) %>%
  pull(participants) %>%
  unique()

cat("Focal firms (Asia + SIC 3571):", length(focal_firms), "\n")

# 3. Find all deals involving focal firms
focal_deals <- sdc_filt %>%
  filter(participants %in% focal_firms,
         participant_nation %in% asia_countries) %>%
  pull(deal_number) %>%
  unique()

cat("Focal deals:", length(focal_deals), "\n")

# 4. Expand to all firms in those deals
all_alliances <- sdc_filt %>%
  filter(deal_number %in% focal_deals) %>%
  select(deal_number, participants, participant_nation, SIC_primary, 
         date_announced)

cat("Unique participants in network:", n_distinct(all_alliances$participants), "\n")

# ----------------------------------------
# 3. Importing PatentsView data (all countries)
# ----------------------------------------
patents_data <- read_parquet("./data/patents.parquet") %>%
  select(patent_number, assignee, country, application_year, grant_year) %>%
  filter(application_year >= 2000)

# Count patents per assignee
patent_counts <- patents_data %>%
  group_by(assignee) %>%
  summarize(num_pat = n(), .groups = "drop")

# Disambiguate assignee names
patent_counts$assignee <- patent_counts$assignee %>%
  trimws() %>%
  standardize_magerman()

cat("Patent assignees:", nrow(patent_counts), "\n")

# ----------------------------------------
# 6. Importing and Cleaning Orbis data (155 companies with extra fields)
# ----------------------------------------
orbis_numeric_cols <- c(
  "employees_last_value",
  "current_assets_usd",
  "n_companies_group",
  "n_shareholders",
  "n_subsidiaries",
  "roa_net_income",
  "total_assets_eur",
  "rnd_over_operating",
  "export_over_operating",
  "long_term_debt_eur"
)

orbis_col_map <- c(
  company_name = "company_name_latin_alphabet",
  country_iso = "country_iso_code",
  city = "city_latin_alphabet",
  nace_code = "nace_rev_2_core_code_4_digits",
  consolidation_code = "consolidation_code",
  last_avail_year = "last_avail_year",
  country = "country",
  address_type = "type",
  peer_group_size = "peer_group_size",
  size_class = "size_classification",
  employees_last_value = "number_of_employees_last_avail_value",
  current_assets_usd = "current_assets_th_usd_last_avail_yr",
  n_companies_group = "no_of_companies_in_corporate_group",
  entity_type = "entity_type",
  n_shareholders = "no_of_shareholders",
  n_subsidiaries = "no_of_subsidiaries",
  roa_net_income = "roa_using_net_income_last_avail_yr",
  total_assets_eur = "total_assets_th_eur_last_avail_yr",
  rnd_over_operating = "r_d_expenses_operating_revenue_last_avail_yr",
  export_over_operating = "export_revenue_operating_revenue_last_avail_yr",
  long_term_debt_eur = "long_term_debt_th_eur_last_avail_yr"
)

orbis_data <- read_excel("./data/Orbis_155_comp_extra_fields.xlsx",
                         sheet = "Results",
                         .name_repair = "minimal") %>%
  rename_with(normalize_orbis_name) %>%
  select(-matches("^unnamed"), -matches("_[0-9]+$")) %>%  # drop placeholder/duplicate cols
  { apply_orbis_col_map(., orbis_col_map) } %>%
  mutate(across(where(is.character), ~ na_if(.x, "n.a."))) %>%
  mutate(across(all_of(orbis_numeric_cols), ~ as.numeric(.x))) %>%
  mutate(
    entity_type = factor(entity_type),
    is_guo = ifelse(entity_type == "GUO", 1, 0)
  ) %>%
  filter(!is.na(employees_last_value),
         employees_last_value > 1)

orbis_data$company_name <- orbis_data$company_name %>%
  standardize_magerman()

cat("Orbis companies loaded:", nrow(orbis_data), "\n")

# ----------------------------------------
# 4b. Load SDC-to-Orbis name mapping
# ----------------------------------------
name_mapping <- read.csv("./data/Orbis type shit.csv", 
                         stringsAsFactors = FALSE) %>%
  rename(sdc_name = `Company.name`, orbis_name = `Orbis.name`) %>%
  mutate(
    orbis_name = trimws(gsub("[\r\n]", "", orbis_name))  
    # Clean multi-line entries
  ) %>%
  filter(orbis_name != "-",
         orbis_name != "",
         orbis_name != "NO MATCH",
         !is.na(orbis_name)) %>%
  mutate(
    sdc_name = trimws(sub(",.*", "", sdc_name)),  # Remove country suffix
    sdc_name = standardize_magerman(sdc_name),
    orbis_name = standardize_magerman(orbis_name)
  )

cat("Name mappings loaded:", nrow(name_mapping), "\n")

# ----------------------------------------
# 5. Build Alliance Network (matching EDA approach)
# ----------------------------------------
# Create firm-firm edge list using combn() - same as EDA
edges <- all_alliances %>%
  group_by(deal_number) %>%
  filter(n_distinct(participants) > 1) %>%
  summarise(
    pairs = list(combn(unique(participants), 2, simplify = FALSE)),
    .groups = "drop"
  ) %>%
  unnest(pairs) %>%
  transmute(
    from = map_chr(pairs, 1),
    to = map_chr(pairs, 2)
  )

# Build graph from edge list
firm_graph <- graph_from_data_frame(edges, directed = FALSE)
firm_graph <- simplify(firm_graph, remove.loops = TRUE, remove.multiple = TRUE)

# Node colors: focal firms (red) vs partners (grey)
V(firm_graph)$color <- ifelse(
  V(firm_graph)$name %in% focal_firms,
  "red",
  "grey70"
)

set.seed(1234)
plot(firm_graph,
     vertex.size = 3,
     vertex.label = NA,
     edge.color = "grey80",
     main = paste0("Strategic Alliances Network\n",
                   "Focal SIC = ", SIC_CODE, ", Continent = Asia\n",
                   "Nodes: ", vcount(firm_graph), " | Edges: ", ecount(firm_graph)))

cat("Network: Nodes =", vcount(firm_graph), ", Edges =", ecount(firm_graph), "\n")

# Network metrics
network_data <- data.frame(
  company_name = V(firm_graph)$name,
  degree = degree(firm_graph),
  betweenness = betweenness(firm_graph),
  is_focal = V(firm_graph)$name %in% focal_firms
)

# ----------------------------------------
# 5b. Compute Geographic Reach (cross-continental share)
# ----------------------------------------
# Use ISO codes + countrycode to map nations to continents
iso_custom <- c("Utd Arab Em" = "ARE", "Hong Kong" = "HKG")

all_alliances <- all_alliances %>%
  mutate(
    participant_iso = countrycode(participant_nation,
                                  origin = "country.name",
                                  destination = "iso3c",
                                  custom_match = iso_custom),
    continent = countrycode(participant_iso,
                            origin = "iso3c",
                            destination = "continent")
  )

all_alliances$continent[is.na(all_alliances$continent)] <- "Other"

# For each firm, compute share of partners from different continents
geo_reach <- all_alliances %>%
  group_by(deal_number) %>%
  mutate(deal_continents = n_distinct(continent)) %>%
  ungroup() %>%
  group_by(participants) %>%
  summarize(
    n_deals = n_distinct(deal_number),
    cross_continental_deals = sum(deal_continents > 1) / n(),
    n_continents = n_distinct(continent),
    .groups = "drop"
  ) %>%
  rename(company_name = participants)

network_data <- network_data %>%
  left_join(geo_reach, by = "company_name")

# ----------------------------------------
# 6. Merge All Data Sources
# ----------------------------------------
# First, add Orbis name mapping to network data
final_data <- network_data %>%
  left_join(name_mapping, by = c("company_name" = "sdc_name")) %>%
  mutate(orbis_match_name = coalesce(orbis_name, company_name))

# Merge with Orbis using mapped names
final_data <- final_data %>%
  left_join(orbis_data, by = c("orbis_match_name" = "company_name"))

# Merge with patent counts
final_data <- final_data %>%
  left_join(patent_counts, by = c("company_name" = "assignee"))

# Fill missing patents with 0
final_data$num_pat <- ifelse(is.na(final_data$num_pat), 0, final_data$num_pat)

cat("Final data rows:", nrow(final_data), "\n")
cat("Rows with Orbis match:", sum(!is.na(final_data$employees_last_value)))
cat("Rows with patent data:", sum(final_data$num_pat > 0), "\n")

# ----------------------------------------
# 6b. Compute Partner Size Diversity (KEY IV)
# ----------------------------------------
# For each firm, get neighbors from network and compute CV of their 
# employee counts

# Create lookup: company_name -> employees (using orbis match)
emp_lookup <- final_data %>%
  select(company_name, employees_last_value) %>%
  filter(!is.na(employees_last_value))

cat("Firms with Orbis employee data:", nrow(emp_lookup), "\n")

# Function to compute partner size diversity for a focal firm
# Returns 0 for single partner (no variance) instead of NA
compute_partner_diversity <- function(focal_name, graph, emp_df) {
  neighbors_names <- neighbors(graph, focal_name, mode = "all")$name
  if (length(neighbors_names) == 0) return(NA_real_)
  
  partner_sizes <- emp_df$employees_last_value[
    emp_df$company_name %in% neighbors_names
  ]
  partner_sizes <- partner_sizes[!is.na(partner_sizes)]
  
  if (length(partner_sizes) == 0) return(NA_real_)
  if (length(partner_sizes) == 1) return(0)  # Single partner = zero variance
  
  # Coefficient of Variation (CV) = SD / Mean
  cv <- sd(partner_sizes) / mean(partner_sizes)
  return(cv)
}

# Compute for all firms in network
final_data$partner_size_diversity <- sapply(
  final_data$company_name,
  function(x) compute_partner_diversity(x, firm_graph, emp_lookup)
)

cat("Firms with partner diversity computed:", 
    sum(!is.na(final_data$partner_size_diversity)), "\n")
cat("Firms missing diversity (no partner data):", 
    sum(is.na(final_data$partner_size_diversity)), "\n")

# Filter out rows without partner size diversity before modeling/export
final_data <- final_data %>%
  filter(!is.na(partner_size_diversity))

cat("Rows retained after dropping NA diversity:", nrow(final_data), "\n")


# ----------------------------------------
# 7. Regression Analysis (Aligned with Hypotheses)
# ----------------------------------------
# H1: Inverted U-shaped effect of partner size diversity
# H2: Geographic reach moderates the relationship
# H3: Betweenness centrality moderates the relationship

# Create squared term for inverted-U test
final_data$partner_diversity_sq <- final_data$partner_size_diversity^2

# ----------------------------------------
# 6c. Export merged dataset for inspection
# ----------------------------------------
write.csv(final_data,
          "./data/final_merged_data.csv",
          row.names = FALSE)
cat("Exported merged dataset to ./data/final_merged_data.csv\n")

# Model 1: Basic controls (firm size via employees, assets; network position)
model_base <- lm(num_pat ~ employees_last_value + total_assets_eur + degree,
                 data = final_data)

# Model 2: Add partner size diversity (linear) + entity type control
model_linear <- lm(num_pat ~ partner_size_diversity + 
                     employees_last_value + total_assets_eur + degree + is_guo,
                   data = final_data)

# Model 3: H1 - Inverted U (add quadratic term)
model_h1 <- lm(num_pat ~ partner_size_diversity + partner_diversity_sq +
                 employees_last_value + total_assets_eur + degree + is_guo,
               data = final_data)

# Model 4: H2 - Geographic moderation
model_h2 <- lm(num_pat ~ partner_size_diversity + partner_diversity_sq +
                 cross_continental_deals +
                 partner_size_diversity:cross_continental_deals +
                 employees_last_value + total_assets_eur + degree + is_guo,
               data = final_data)

# Model 5: H3 - Betweenness moderation
model_h3 <- lm(num_pat ~ partner_size_diversity + partner_diversity_sq +
                 betweenness +
                 partner_size_diversity:betweenness +
                 employees_last_value + total_assets_eur + degree + is_guo,
               data = final_data)

# Model 6: Full model with all hypotheses
model_full <- lm(num_pat ~ partner_size_diversity + partner_diversity_sq +
                   cross_continental_deals + betweenness +
                   partner_size_diversity:cross_continental_deals +
                   partner_size_diversity:betweenness +
                   employees_last_value + total_assets_eur + degree + is_guo,
                 data = final_data)

# Model 7: Entity type as moderator (GUO vs subsidiary)
model_entity <- lm(num_pat ~ partner_size_diversity + partner_diversity_sq +
                     is_guo + partner_size_diversity:is_guo +
                     employees_last_value + total_assets_eur + degree,
                   data = final_data)

cat("\n=== MODEL RESULTS ===\n")
cat("\n--- Model 1: Base Controls ---\n")
print(summary(model_base))

cat("\n--- Model 2: Linear Partner Diversity ---\n")
print(summary(model_linear))

cat("\n--- Model 3: H1 Inverted-U ---\n")
print(summary(model_h1))

cat("\n--- Model 4: H2 Geographic Moderation ---\n")
print(summary(model_h2))

cat("\n--- Model 5: H3 Betweenness Moderation ---\n")
print(summary(model_h3))

cat("\n--- Model 6: Full Model ---\n")
print(summary(model_full))

cat("\n--- Model 7: Entity Type Moderation ---\n")
print(summary(model_entity))