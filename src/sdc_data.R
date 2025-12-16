install.packages("config")

library(dplyr)
library(tidyr)
library(igraph)
library(purrr)
library(countrycode)
library(janitor)
library(config)

process_and_save_sdc <- function(rds_path, target_sic, target_continent, out_path,
                                  min_date = NULL, exclude_terminated = TRUE) {
  # 1. Load data and clean column names
  sdc <- readRDS(rds_path) %>%
    as_tibble() %>%
    janitor::clean_names()

  # 2. Drop rows where participants are NA/empty
  sdc_clean <- sdc %>%
    mutate(participants = participants %>% trimws() %>% na_if("") %>% na_if("NA") %>% na_if("<NA>")) %>%
    filter(!is.na(participants))

  sdc_clean <- sdc_clean %>%
  filter(
    status == "Completed/Signed",
    type == "Strategic Alliance",
    date_terminated == "",
    date_announced > min_date,
    !is.na(participants),
    !is.na(deal_number)
  )

  # 5. Assign continent based on participant_nation
  sdc_clean <- sdc_clean %>%
    mutate(
      participant_country = trimws(participant_nation),
      continent = countrycode(participant_country, "country.name", "continent", warn = FALSE)
    )

  # 6. Print and drop rows where continent couldn't be determined
  missing_continent <- sdc_clean %>%
    filter(is.na(continent)) %>%
    distinct(participant_country) %>%
    pull(participant_country)

  if (length(missing_continent) > 0) {
    message("Dropped countries/regions without continent mapping: ", paste(missing_continent, collapse = ", "))
  }

  sdc_clean <- sdc_clean %>%
    filter(!is.na(continent))

  # 7. Find focal FIRMS: unique company names that appear in target_continent with target_sic
  focal_firms <- sdc_clean %>%
    filter(sic_primary == target_sic, continent == target_continent) %>%
    pull(participants) %>%
    unique()

  # 8. Find deals where focal firms participate FROM the target continent
  #    (not just any deal the focal firm is in)
  relevant_deals <- sdc_clean %>%
    filter(
      participants %in% focal_firms,
      continent == target_continent
    ) %>%
    distinct(deal_number) %>%
    pull(deal_number)

  # 9. Expand: include ALL participants in those deals (may be outside target_continent)
  result_df <- sdc_clean %>%
    filter(deal_number %in% relevant_deals)

  # 10. Log summary statistics
  focal_company_count <- length(focal_firms)

  total_companies <- result_df %>%
    distinct(participants) %>%
    nrow()

  total_deals <- length(relevant_deals)

  message("Focal companies (sic_primary=", target_sic, ", continent=", target_continent, "): ", focal_company_count)
  message("Total companies in expanded network: ", total_companies)
  message("Total deals involving focal companies (from ", target_continent, "): ", total_deals)

  # 11. Save to file
  write.csv(result_df, out_path, row.names = FALSE)

  result_df
}

plot_network <- function(dataframe, target_sic, target_continent) {
  focal_firms <- dataframe %>%
    filter(sic_primary == target_sic, continent == target_continent) %>%
    pull(participants) %>%
    unique()

  # Create firm-firm edge list
  edges <- dataframe %>%
    group_by(deal_number) %>%
    filter(n_distinct(participants) > 1) %>%
    summarise(
      pairs = list(combn(unique(participants), 2, simplify = FALSE)),
      .groups = "drop"
    ) %>%
    unnest(pairs) %>%
    transmute(
      from = map_chr(pairs, 1),
      to   = map_chr(pairs, 2)
    )

  # Visualize network
  g <- graph_from_data_frame(edges, directed = FALSE)

  # Node colors: focal vs partners
  V(g)$color <- ifelse(
    V(g)$name %in% focal_firms,
    "red",
    "grey70"
  )

  # Plot
  set.seed(1234)
  plot(
    g,
    vertex.size = 3,
    vertex.label = NA,
    edge.color = "grey80",
    main = paste0(
      "Strategic Alliances Network\n",
      "Focal SIC = ", target_sic, ", Continent = ", target_continent, "\n",
      "Nodes: ", vcount(g), " | Edges: ", ecount(g)
    )
  )

  # Input for company filtering
  cat(
    unique(
      paste(dataframe$participants)
    ),
    sep = "\n"
  )

  # Checking duplicates
  dataframe %>%
    distinct(participants, participant_nation) %>%
    group_by(participants) %>%
    filter(n_distinct(participant_nation) > 1) %>%
    arrange(participants, participant_nation)

  invisible(g)
}

cfg <- config::get()

# Access values
focal_industry_sic_code <- cfg$focal_industry_sic_code
continent <- cfg$continent
min_date <- cfg$min_date

# Example call with date and termination filters
result <- process_and_save_sdc(
  rds_path = cfg$sdc_raw_file,
  target_sic = focal_industry_sic_code,
  target_continent = continent,
  out_path = cfg$sdc_filtered_file,
  min_date = min_date,
  exclude_terminated = TRUE
)
plot_network(result, target_sic = focal_industry_sic_code, target_continent = continent)