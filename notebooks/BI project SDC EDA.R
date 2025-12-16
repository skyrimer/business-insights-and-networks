### BUSINESS AND INSIGHTS - GROUP ASSIGNMENT - EDA


# Setup -------------------------------------------------------------------
# Import libraries
library(tidyr)
library(dplyr)
library(igraph)
library(purrr)

# Set working directory (change to your own path)
setwd("C:\\Users\\20234582\\OneDrive - TU Eindhoven\\JBE140 (2025-2) Business Insights & Networks")

# Section SDC -------------------------------------------------------------
# Import SDC data
sdc_data <- readRDS("SDC_data_2021.rds")

# List of Asian countries
asia <- c("Japan","Iran","India","Indonesia","Malaysia","Pakistan","Thailand",
          "Taiwan","Vietnam","China","South Korea","Philippines","Singapore",
          "Uzbekistan","Brunei","Myanmar(Burma)","Laos","Cambodia","Mongolia",
          "Afghanistan","Kazakhstan","North Korea","Bangladesh","Sri Lanka",
          "Armenia","Turkmenistan","Kyrgyzstan","Nepal","Maldives","Bhutan",
          "Timor-Leste","Oman","Yemen","Jordan","Israel","Lebanon","Iraq",
          "Bahrain","Qatar","Utd Arab Em","Palestine","Cyprus","Georgia","Turkey")

# Parameters
SIC_code <- 3571
continent_name <- "Asia"

# 1. Filter to strategic alliances
sdc_filt <- sdc_data %>%
  filter(
    status == "Completed/Signed",
    type == "Strategic Alliance",
    date_terminated == "", 
    date_announced > "1999-12-31",  
    !is.na(participants),
    !is.na(deal_number)
  )

# Disambiguation of company names (perhaps not the most optimal place to do it)
#install.packages("devtools")
#devtools::install_github("stasvlasov/nstandr")
#library(nstandr)
#sdc_filt$participants <- sdc_filt$participants %>% standardize_magerman()

# 2. Identify focal firms
focal_firms <- sdc_filt %>%
  filter(
    SIC_primary == SIC_code,
    participant_nation %in% asia
  ) %>%
  pull(participants) %>%
  unique()

# 3. Find all deals involving focal firms
focal_deals <- sdc_filt %>%
  filter(participants %in% focal_firms, 
         participant_nation %in% asia) %>%
  pull(deal_number) %>%
  unique()

# 4. Expand to all firms in those deals
network_data <- sdc_filt %>%
  filter(deal_number %in% focal_deals) %>%
  select(deal_number, participants, participant_nation)

# 5. Create firm-firm edge list
edges <- network_data %>%
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
    "Focal SIC = ", SIC_code, ", Continent = ", continent_name, "\n",
    "Nodes: ", vcount(g), " | Edges: ", ecount(g)
  )
)

# Input for company filtering
cat(
  unique(
    paste(network_data$participants)
  ),
  sep = "\n"
)

# Checking duplicates
network_data %>%
  distinct(participants, participant_nation) %>%
  group_by(participants) %>%
  filter(n_distinct(participant_nation) > 1) %>%
  arrange(participants, participant_nation)
  # There are no duplicates




