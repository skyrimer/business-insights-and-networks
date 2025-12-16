### BY JUNIOR

library(tidyverse)
library(janitor)
library(igraph)
library(ggraph)
library(tidygraph)

setwd("C:/Users/Junior/OneDrive - TU Eindhoven/year 3/bus/GA")

sdc <- readRDS("data/SDC_data_2021.rds") %>%
  clean_names() %>%
  mutate(
    participants = participants %>%
      trimws() %>% na_if("") %>% na_if("NA") %>% na_if("<NA>")
  )

sdc_filtered <- sdc %>%
  filter(
    status == "Completed/Signed",
    type == "Strategic Alliance"
  )

continent_map <- list(
  Europe = c("United Kingdom","Germany","France","Italy","Spain","Netherlands",
             "Belgium","Sweden","Norway","Finland","Denmark","Austria",
             "Switzerland","Ireland","Portugal"),
  Asia = c("Japan","China","South Korea","India","Singapore","Hong Kong",
           "Taiwan","Malaysia","Philippines","Indonesia","Thailand"),
  North_America = c("United States","Canada","Mexico"),
  South_America = c("Brazil","Argentina","Chile","Colombia","Peru"),
  Oceania = c("Australia","New Zealand"),
  Africa = c("South Africa","Egypt","Nigeria","Kenya")
)

continent_lookup <- enframe(continent_map, name = "continent", value = "country") %>%
  unnest(country)

sdc_filtered_geo <- sdc_filtered %>%
  left_join(continent_lookup, by = c("participant_nation" = "country")) %>%
  filter(!is.na(participants)) %>%
  rename(firm = participants)

# Firm metadata
firm_info <- sdc_filtered_geo %>%
  select(firm, sic_primary, continent) %>%
  distinct()

edge_list <- sdc_filtered_geo %>%
  group_by(deal_number) %>%
  summarise(firms = list(unique(firm)), .groups = "drop") %>%
  mutate(n_firms = map_int(firms, length)) %>%
  filter(n_firms >= 2) %>%
  mutate(pairs = map(firms, ~ as.data.frame(t(combn(.x, 2))))) %>%
  unnest(pairs) %>%
  rename(from = V1, to = V2) %>%
  distinct()

clean_firm <- function(x) {
  x %>% iconv(to = "ASCII//TRANSLIT", sub = "") %>% trimws() %>% na_if("")
}

edges_final <- edge_list %>%
  mutate(from = clean_firm(from), to = clean_firm(to)) %>%
  drop_na() %>%
  distinct()

edges_with_info <- edges_final %>%
  left_join(firm_info, by = c("from" = "firm")) %>%
  rename(from_sic = sic_primary, from_region = continent) %>%
  left_join(firm_info, by = c("to" = "firm")) %>%
  rename(to_sic = sic_primary, to_region = continent)



make_region_table <- function(sic_code) {
  
  # Firms in this SIC
  firms_sic <- sdc_filtered_geo %>% 
    filter(sic_primary == sic_code)
  
  # --- Number of firms per region ---
  firms_per_region <- firms_sic %>%
    group_by(continent) %>%
    summarise(Number_of_Firms = n_distinct(firm), .groups = "drop")
  
  # --- Number of deals per region ---
  deals_per_region <- firms_sic %>%
    group_by(continent) %>%
    summarise(Number_of_Deals = n_distinct(deal_number), .groups = "drop")
  
  # Merge into one table
  region_table <- firms_per_region %>%
    left_join(deals_per_region, by = "continent") %>%
    arrange(desc(Number_of_Firms))
  
  region_table
}
table_3663 <- make_region_table(3663)
table_7379 <- make_region_table(7379)
table_3679 <- make_region_table(3679)
table_6211 <- make_region_table(6211)
table_3663
table_7379
table_3679
table_6211



plot_sic_bipartite <- function(data, sic_code, region = NULL, title_region = NULL) {
  
  # 1. Filter by SIC
  df <- data %>% filter(SIC_primary == sic_code)
  
  # 2. Filter by region if requested
  if (!is.null(region)) {
    df <- df %>% filter(participant_nation %in% region)
  }
  
  if (nrow(df) == 0) {
    message("No data for this SIC × region.")
    return(NULL)
  }
  
  # 3. Build bipartite incidence matrix (same method as your code)
  bip <- df %>%
    select(participants, deal_number) %>%
    as.matrix()
  
  g <- graph_from_data_frame(bip, directed = FALSE)
  
  # Assign bipartite types
  V(g)$type <- V(g)$name %in% bip[,2]
  
  # 4. Project onto participants
  g_proj <- bipartite_projection(g)$proj1
  
  # 5. Remove loops & duplicates
  g_proj <- simplify(g_proj, remove.multiple = TRUE, remove.loops = TRUE)
  
  # 6. Plot (same visual style as original)
  set.seed(2001525)
  plot(
    g_proj,
    vertex.color = "coral2",
    vertex.label = NA,
    vertex.size = 5,
    edge.width = 2,
    edge.color = adjustcolor("black", alpha.f = 0.3),
    main = paste0(
      "Network — SIC ", sic_code, 
      if (!is.null(title_region)) paste0(" (", title_region, ")"),
      "\nNodes: ", vcount(g_proj),
      " | Edges: ", ecount(g_proj)
    )
  )
  
  return(g_proj)
}

sdc_clean <- sdc_data %>%
  filter(
    status == "Completed/Signed",
    type == "Strategic Alliance"
  ) %>%
  mutate(participants = participants %>% trimws() %>% na_if(""))

g_7379_eu <- plot_sic_bipartite(
  data = sdc_clean,
  sic_code = 7379,
  region = continent_map$Europe,
  title_region = "Europe"
)
g_3663_asia <- plot_sic_bipartite(
  data = sdc_clean,
  sic_code = 3663,
  region = continent_map$Asia,
  title_region = "Asia"
)
g_3679_na <- plot_sic_bipartite(
  data = sdc_clean,
  sic_code = 3679,
  region = continent_map$Asia,
  title_region = "Asia"
)


g_6211_asia <- plot_sic_bipartite(
  data         = sdc_clean,
  sic_code     = 6211,
  region       = continent_map$Asia,
  title_region = "Asia"
)


### BY TRINITY
### BUSINESS AND INSIGHTS - GROUP ASSIGNMENT - EDA

# Import libraries
library(tidyr)
library(dplyr)
library(igraph)

# Set working directory (change to your own path)
setwd("C:\\Users\\20234582\\OneDrive - TU Eindhoven\\JBE140 (2025-2) Business Insights & Networks")

## SECTION SDC
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

SIC_code <- 3674

# Subset Asian alliances with SIC_primary = 3679
asia_alliances <- sdc_data %>%
  filter(
    status == "Completed/Signed",
    date_terminated == "",
    type == "Strategic Alliance",
    participant_nation %in% asia,
    type=="Strategic Alliance",
    SIC_primary == SIC_code | alliance_SIC_code == SIC_code
  )



asia_alliances$participants <- asia_alliances$participants %>% standardize_magerman()

# Number of unique participants
num_unique_participants <- n_distinct(asia_alliances$participants)

# Range of date_announced
date_range <- range(asia_alliances$date_announced, na.rm = TRUE)

# Alliance nation counts: in Asia
alliance_in_asia <- sum(asia_alliances$alliance_nation %in% asia, na.rm = TRUE)

# Status value counts
status_counts <- table(asia_alliances$status, useNA = "ifany")

# Type value counts
type_counts <- table(asia_alliances$type, useNA = "ifany")

# Alliance SIC code value counts
alliance_sic_counts <- sort(table(asia_alliances$alliance_SIC_code, useNA = "ifany"), decreasing = TRUE)

# Combine results into a summary list
asia_summary <- list(
  "Unique Participants" = num_unique_participants,
  "Date Announced Range" = paste(date_range[1], "to", date_range[2]),
  "Alliances in Asia" = alliance_in_asia,
  "Status Counts" = status_counts,
  "Type Counts" = type_counts,
  "Alliance SIC Counts" = alliance_sic_counts
)


# Visualizing the network
asia_alliances_net <- asia_alliances %>%
  select(participants, deal_number) %>%
  as.matrix()

asia_alliances_graph <- graph_from_data_frame(asia_alliances_net, directed=FALSE)

V(asia_alliances_graph)$type <- ifelse(V(asia_alliances_graph)$name %in%
                                         asia_alliances_net[,2],
                                       yes=TRUE, no=FALSE)

asia_alliances_graph <- bipartite_projection(asia_alliances_graph)$proj1

asia_alliances_graph <- simplify(asia_alliances_graph, remove.loops=TRUE,
                                 remove.multiple=TRUE)

set.seed(2001525)
plot(asia_alliances_graph, vertex.color="coral2",
     vertex.label=NA, vertex.size=3, edge_width=3,
     edge.color=adjustcolor("black", alpha.f=0.7),
     main=paste0("Network: SIC", SIC_code, "(Asia)\nNodes: ",
                 vcount(asia_alliances_graph),
                 " | Edges: ", ecount(asia_alliances_graph))
)

# Create node metrics data frame
asia_alliances_data <- data.frame(
  company_name = V(asia_alliances_graph)$name,
  degree = degree(asia_alliances_graph),
  betweenness = betweenness(asia_alliances_graph)
)




