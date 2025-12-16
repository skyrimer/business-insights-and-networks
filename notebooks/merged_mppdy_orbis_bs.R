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
if (!requireNamespace("arrow", quietly = TRUE)) {
  install.packages("arrow")
}
library(arrow)

# We will use the "nstandr" package and the magerman method
if (!requireNamespace("nstandr", quietly = TRUE)) {
  if (!requireNamespace("devtools", quietly = TRUE)) {
    install.packages("devtools")
  }
  devtools::install_github("stasvlasov/nstandr")
}
library(nstandr)

# ----------------------------------------
# 2. Load and Filter SDC Data
# ----------------------------------------
sdc_data <- readRDS("./data/SDC_data_2021.rds")

# Asia countries list
asia_countries <- c("Japan", "China", "South Korea", "India", "Singapore",
                    "Hong Kong", "Taiwan", "Malaysia", "Philippines",
                    "Indonesia", "Thailand", "Vietnam")
SIC_CODE <- 3571

# Find focal deals: Asia participants with SIC 3571, after 2000
focal_alliances <- sdc_data %>%
  filter(status == "Completed/Signed",
         date_terminated == "",
         type == "Strategic Alliance",
         date_announced >= "2000-01-01",
         participant_nation %in% asia_countries,
         SIC_primary == SIC_CODE | alliance_SIC_code == SIC_CODE)

focal_deals <- unique(focal_alliances$deal_number)

# Expand: get ALL participants in those deals (including non-Asia partners)
all_alliances <- sdc_data %>%
  filter(deal_number %in% focal_deals) %>%
  select(participants, deal_number, participant_nation, SIC_primary, date_announced)

# Disambiguate names
all_alliances$participants <- all_alliances$participants %>%
  trimws() %>%
  standardize_magerman()

cat("Focal deals:", length(focal_deals), "\n")
cat("Unique participants:", n_distinct(all_alliances$participants), "\n")

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
# 6. Importing and Cleaning Orbis data (subset)
# ----------------------------------------
# Importing Orbis data (all companies located in the US with NAICS 394 & 395)
# Columns are renamed to concise snake_case to keep key metrics (employees,
# subsidiaries, ROA, assets, R&D, revenue, sales, debt) consistent throughout
# the pipeline.

orbis_numeric_cols <- c(
  "employees_last_year",
  "subsidiaries_total",
  "subsidiaries_ultimate",
  "roa_net_income_2019",
  "employees_2019",
  "total_assets_m_eur_2019",
  "rnd_expenses_over_operating",
  "export_rev_over_operating",
  "sales_m_eur_2019",
  "long_term_debt_m_eur_2019"
)

orbis_data <- read_excel("./data/Export 16_12_2025 11_39.xlsx",
                         sheet = "Results") %>%
  select(-starts_with("..."), -any_of("Unnamed: 0")) %>%
  rename(
    company_name = `Company name Latin alphabet`,
    country_iso = `Country ISO code`,
    employees_last_year = `Number of employees\nLast avail. yr`,
    subsidiaries_total = `No of subsidiaries`,
    subsidiaries_ultimate = `No of subsidiaries (ultimately-owned included)`,
    roa_net_income_2019 = `ROA using Net income\n2019`,
    employees_2019 = `Number of employees\n2019`,
    total_assets_m_eur_2019 = `Total assets\nm EUR 2019`,
    rnd_expenses_over_operating = `R&D expenses / Operating revenue\n2019`,
    export_rev_over_operating = `Export revenue / Operating revenue\n2019`,
    sales_m_eur_2019 = `sales\nm EUR 2019`,
    long_term_debt_m_eur_2019 = `Long term debt\nm EUR 2019`
  ) %>%
  mutate(across(where(is.character), ~ na_if(.x, "n.a."))) %>%
  mutate(across(all_of(orbis_numeric_cols), ~ as.numeric(.x))) %>%
  filter(!is.na(employees_last_year),
         employees_last_year > 1,
         !is.na(sales_m_eur_2019),
         sales_m_eur_2019 > 0)

orbis_data$company_name <- orbis_data$company_name %>%
  standardize_magerman()

# ----------------------------------------
# 4b. Load SDC-to-Orbis name mapping
# ----------------------------------------
name_mapping <- read.csv("./data/Orbis type shit.csv", stringsAsFactors = FALSE) %>%
  rename(sdc_name = `Company.name`, orbis_name = `Orbis.name`) %>%
  filter(orbis_name != "-") %>%
  mutate(
    sdc_name = trimws(sub(",.*", "", sdc_name)),  # Remove country suffix
    sdc_name = standardize_magerman(sdc_name),
    orbis_name = standardize_magerman(orbis_name)
  )

cat("Name mappings loaded:", nrow(name_mapping), "\n")

# ----------------------------------------
# 5. Build Alliance Network
# ----------------------------------------
alliance_net <- all_alliances %>%
  select(participants, deal_number) %>%
  distinct() %>%
  as.matrix()

alliance_graph <- graph_from_data_frame(alliance_net, directed = FALSE)
V(alliance_graph)$type <- V(alliance_graph)$name %in% alliance_net[, 2]

# Project to firm-firm network
firm_graph <- bipartite_projection(alliance_graph)$proj1
firm_graph <- simplify(firm_graph, remove.loops = TRUE, remove.multiple = TRUE)

set.seed(2001525)
plot(firm_graph, vertex.color = "coral2",
     vertex.label = NA, vertex.size = 5, edge.width = 1,
     edge.color = adjustcolor("black", alpha.f = 0.3),
     main = paste0("Alliance Network: SIC ", SIC_CODE, " (Asia focal)\n",
                   "Nodes: ", vcount(firm_graph), " | Edges: ", ecount(firm_graph)))

# Network metrics
network_data <- data.frame(
  company_name = V(firm_graph)$name,
  degree = degree(firm_graph),
  betweenness = betweenness(firm_graph)
)

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
cat("Rows with Orbis match:", sum(!is.na(final_data$employees_last_year)), "\n")
cat("Rows with patent data:", sum(final_data$num_pat > 0), "\n")

# ----------------------------------------
# 7. Running regression analysis
# ----------------------------------------
# shitty model: innovation ===== network position + firm size
model <- lm(num_pat ~ degree + betweenness + employees_last_year + sales_m_eur_2019,
            data = final_data)
summary(model)


library(glue)

source_counts <- tibble(
  source = c("SDC focal + partner participants",
             "Orbis sheet (after cleaning filter)",
             "PatentsView assignees (>=2000)"),
  n_companies = c(n_distinct(all_alliances$participants),
                  n_distinct(orbis_data$company_name),
                  n_distinct(patent_counts$assignee))
)

intersection_counts <- final_data %>%
  mutate(
    has_orbis = !is.na(employees_last_year),
    has_patents = num_pat > 0,
    regression_ready = complete.cases(
      degree, betweenness, employees_last_year, sales_m_eur_2019, num_pat
    )
  ) %>%
  summarise(
    total_final_nodes = n_distinct(company_name),
    with_orbis = sum(has_orbis),
    with_patents = sum(has_patents),
    regression_n = sum(regression_ready)
  )

regression_firms <- final_data %>%
  mutate(
    has_orbis = !is.na(employees_last_year),
    has_patents = num_pat > 0,
    regression_ready = complete.cases(
      degree, betweenness, employees_last_year, sales_m_eur_2019, num_pat
    )
  ) %>%
  filter(regression_ready) %>%
  arrange(desc(num_pat)) %>%
  select(company_name, orbis_match_name, num_pat, employees_last_year,
         sales_m_eur_2019, degree, betweenness)

print(source_counts)
print(intersection_counts)
print(regression_firms$company_name)