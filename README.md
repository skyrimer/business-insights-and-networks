# Business Insights and Networks

Research project analyzing the relationship between alliance networks, partner diversity, and firm innovation outcomes.

## Data Pipeline

### 1. Data Preprocessing Scripts

Run these scripts in order to prepare the datasets:

#### `src/patents_data.R`
- Loads raw patent data from multiple CSV files
- Cleans and filters patent applications
- Aggregates patent metrics by assignee (company):
  - Total applications and granted patents
  - Grant success rate
  - Technology specialization (HHI)
  - IPC scope (breadth of technology classes)
  - Team size metrics
- Saves processed patent data to `data/firm_patent_variables.parquet`

#### `src/sdc_data.R`
- Processes SDC Platinum alliance/JV data from RDS format
- Filters strategic alliances by:
  - Status: Completed/Signed
  - Type: Strategic Alliance
  - Industry: Target SIC codes (configurable)
  - Geography: Target countries (configurable)
  - Date range (configurable)
- Adds continent classification
- Saves filtered data to `data/filtered_sdc.csv`

#### `src/orbis_data.R`
- Loads and cleans ORBIS company data from Excel
- Standardizes column names and company names
- Extracts key firm characteristics:
  - Employee count
  - Total assets (EUR/USD)
  - Financial metrics (ROA, R&D ratio, export ratio)
  - Organizational structure (GUO status, subsidiaries, shareholders)
  - Size classification
- **Applies SDC-ORBIS mapping**: Renames ORBIS companies to match SDC format
  - Removes country suffix from company names (e.g., "Dell Inc, United States" → "Dell Inc")
- Saves cleaned data to `data/orbis_cleaned.csv`

#### `src/merge_regression_dataframe.R`
- Merges all three datasets (SDC + ORBIS + Patents)
- **ORBIS merge**: Direct join on company names (pre-renamed in orbis_data.R)
- **Patent matching**: Multi-phase fuzzy matching approach
  - Phase 1: Exact matching on standardized names
  - Phase 2: Fuzzy matching using hybrid scoring (Jaro-Winkler + Levenshtein + Jaccard)
  - Phase 3: Secondary fuzzy matching for unmatched firms
- Standardizes company names using Magerman method (via nstandr package)
- Saves merged dataset to:
  - `data/sdc_patents_orbis_merged.csv`
  - `data/sdc_patents_orbis_merged.parquet`

### 2. Analysis Script

#### `src/regression_analysis.R`
- Loads merged dataset
- Constructs alliance network using igraph
- Computes network metrics:
  - Degree centrality
  - Betweenness centrality
  - Closeness centrality
  - Eigenvector centrality
- Computes geographic reach metrics:
  - Cross-continental alliance share
  - Number of continents reached
- Aggregates to firm-level
- **Computes key independent variables**:
  - Partner size diversity (coefficient of variation)
  - Partner metrics (count, avg/min/max size)
- Creates analysis sample with required filters
- Runs regression models testing hypotheses:
  - H1: Inverted-U relationship between partner diversity and innovation
  - H2: Geographic reach as moderator
  - H3: Network centrality effects
- Saves results:
  - Analysis sample: `data/analysis_sample.parquet` and `.csv`
  - Model comparison: `data/model_comparison.csv`
  - Full model objects: `data/regression_models.rds`

## Configuration

Project uses `config.yml` for paths and parameters. Edit this file to customize:
- Input file paths
- Output file paths
- SIC code filters
- Country filters
- Date ranges

## Dependencies

R packages required:
- tidyverse (dplyr, tidyr, purrr)
- arrow (parquet file support)
- igraph (network analysis)
- countrycode (geographic classification)
- stringdist (fuzzy matching)
- nstandr (company name standardization - Magerman method)
- config (configuration management)
- readxl (Excel file reading)
- janitor (column name cleaning)

Install with:
```r
install.packages(c("tidyverse", "arrow", "igraph", "countrycode",
                   "stringdist", "config", "readxl", "janitor"))
devtools::install_github("stasvlasov/nstandr")
```

## Required Data Files

### Input Files
- `raw_downloads/patents/*.csv` - Raw patent data files
- `raw_downloads/sdc_alliances.rds` - SDC Platinum alliance data
- `raw_downloads/orbis_data.xlsx` - ORBIS company data
- `raw_downloads/sdc_to_orbis_map.csv` - Manual mapping between SDC and ORBIS company names

### Output Files
All output files are saved to `data/` directory.

## Execution Order

1. `src/patents_data.R` - Process patent data
2. `src/sdc_data.R` - Filter alliance data
3. `src/orbis_data.R` - Clean ORBIS data and apply mapping
4. `src/merge_regression_dataframe.R` - Merge all datasets
5. `src/regression_analysis.R` - Run network and regression analysis