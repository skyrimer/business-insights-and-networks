# Business Insights and Networks

Research project analyzing the relationship between alliance networks, partner diversity, and firm innovation outcomes.

## Project Structure

```
business-insights-and-networks/
├── rds/                           # Main R scripts (execution order)
│   ├── patents_data.R             # 1. Process patent data
│   ├── sdc_data.R                 # 2. Filter alliance data
│   ├── orbis_data.R               # 3. Clean ORBIS company data
│   ├── merge_regression_dataframe.R  # 4. Merge all datasets
│   ├── regression_analysis.R      # 5. Run network & regression analysis
│   └── reporting_utils.R          # Utility functions for console output
├── data/                          # Raw input data
│   ├── Export 17_12_2025 18_26.xlsx  # ORBIS company data
│   ├── SDC_data_2021.rds          # SDC Platinum alliance data
│   └── patents_view/              # Raw patent CSV files
├── working_data/                  # Processed output files
│   ├── orbis_cleaned.csv
│   ├── filtered_sdc.csv
│   ├── firm_patent_variables.csv
│   ├── sdc_patents_orbis_merged.csv
│   ├── analysis_sample.csv
│   ├── model_comparison.csv
│   └── regression_models.rds
├── extra/                         # Auxiliary files
│   └── sdc_to_orbis_map.csv      # Manual company name mapping
└── config.yml                     # Configuration parameters
```

## Data Pipeline

### 1. Data Preprocessing Scripts

Run these scripts in the following order:

#### `rds/patents_data.R`
- Loads raw patent data from multiple CSV files in `data/patents_view/`
- Cleans and filters patent applications (removes unknowns, outliers)
- Aggregates patent metrics by assignee (company):
  - Total applications and granted patents
  - Grant success rate
  - Technology specialization (HHI index across IPC sections)
  - IPC scope (breadth of technology classes)
  - Average team size
  - Share of technology class G (Physics/Computing)
- Creates IPC section dummies (A-H)
- Saves processed patent data to `working_data/firm_patent_variables.csv`

#### `rds/sdc_data.R`
- Processes SDC Platinum alliance/JV data from RDS format
- Filters strategic alliances by:
  - Status: Completed/Signed
  - Type: Strategic Alliance
  - Industry: Target SIC codes (configurable via `config.yml`)
  - Geography: Target countries (configurable)
  - Date range (post `min_date` in config)
  - Excludes terminated deals
- Identifies focal firms (target SIC + country)
- Expands network to include all partners in deals with focal firms
- Adds continent classification using countrycode
- Creates alliance network visualization
- Saves filtered data to `working_data/filtered_sdc.csv`

#### `rds/orbis_data.R`
- Loads and cleans ORBIS company data from Excel (sheet: "Results")
- Normalizes column names for consistency
- Uses pattern matching to identify and map columns dynamically
- Extracts key firm characteristics:
  - Employee count (last available value)
  - Total assets (EUR/USD, thousands)
  - Financial metrics (ROA, R&D ratio, export ratio)
  - Organizational structure (GUO status, subsidiaries, shareholders, group size)
  - Size classification
  - Entity type (creates GUO and controlled subsidiary flags)
- Standardizes company names using Magerman method (nstandr package)
- **Applies SDC-ORBIS mapping** from `extra/sdc_to_orbis_map.csv`:
  - Removes country suffix from SDC names (e.g., "Dell Inc, United States" → "Dell Inc")
  - Renames ORBIS companies to match SDC format
- Saves cleaned data to `working_data/orbis_cleaned.csv`

#### `rds/merge_regression_dataframe.R`
- Merges all three datasets (SDC + ORBIS + Patents)
- **ORBIS merge**: Direct left join on company names (pre-renamed in `orbis_data.R`)
- **Patent matching**: Multi-phase fuzzy matching approach
  - Phase 1: Exact matching on standardized names (Magerman method)
  - Phase 2: Hybrid fuzzy matching with blocking (3-char prefix)
    - Jaro-Winkler distance (weight: 0.5)
    - Levenshtein distance (weight: 0.3)
    - Jaccard similarity on tokens (weight: 0.2)
    - Threshold: 0.85 (configurable)
  - Progress bar for tracking fuzzy matching
- Reports match statistics for ORBIS and patents (exact/fuzzy/unmatched)
- Saves merged dataset to `working_data/sdc_patents_orbis_merged.csv`

### 2. Network & Regression Analysis

#### `rds/regression_analysis.R`
- Loads merged dataset from `working_data/sdc_patents_orbis_merged.csv`
- **Network Construction**:
  - Creates firm-to-firm alliance network using igraph
  - Simplifies to undirected graph (removes duplicates)
- **Network Metrics** (computed for each firm):
  - Degree centrality (number of direct partners)
  - Betweenness centrality (normalized)
  - Closeness centrality (normalized)
  - Eigenvector centrality
- **Geographic Reach Metrics**:
  - Cross-continental alliance share (H2 moderator)
  - Number of continents reached
  - Number of cross-continental deals
- **Partner Size Diversity** (H1 main independent variable):
  - Coefficient of variation (CV) of partner employee sizes
  - Requires employee data for partners (ORBIS)
  - Computed using network neighbors
- **Additional Partner Metrics**:
  - Partner count
  - Average, max, min partner size
- **Firm-level Aggregation**:
  - Aggregates all alliance-level data to firm-level
  - Merges ORBIS, patent, network, and geographic data
- **Regression Variables**:
  - Log-transforms for skewed variables (employees, assets, debt)
  - Quadratic term for partner size diversity
  - Creates clean versions of financial ratios
- **Analysis Sample Creation**:
  - Filters firms with:
    - Non-missing partner size diversity
    - Non-missing employee data
    - At least one alliance partner
- **Regression Models** (6 models):
  - **Model 1**: Base controls (size, assets, degree, experience, tech specialization, ROA, R&D)
  - **Model 2**: Linear partner size diversity
  - **Model 3 (H1)**: Inverted-U relationship (quadratic term)
    - Tests if innovation peaks at moderate diversity
  - **Model 4 (H2)**: Geographic reach moderation
    - Tests if cross-continental alliances strengthen the inverted-U
  - **Model 5 (H3)**: Betweenness centrality moderation
    - Tests if network position buffers against extreme diversity costs
  - **Model 6**: Full model (all hypotheses)
- **Outputs**:
  - Descriptive statistics and correlation matrix
  - Hypothesis test results with expected signs
  - Model comparison table (R², Adj R², AIC, BIC)
  - Saves to:
    - `working_data/analysis_sample.csv` - Final analysis sample
    - `working_data/model_comparison.csv` - Model fit statistics
    - `working_data/regression_models.rds` - Full model objects (for further analysis)

#### `rds/reporting_utils.R`
- Utility functions for formatted console output:
  - `print_header()`, `print_section()`, `print_subheader()`
  - `print_success()`, `print_info()`, `print_warning()`, `print_error()`
  - `print_metrics()` - Formatted key-value pairs
  - `print_match_summary()` - Matching statistics with visual bar
  - `create_progress_bar()`, `update_progress()` - Progress tracking
  - `print_summary_table()` - Formatted data tables
  - `print_model_comparison()` - Model fit comparison table
  - `print_hypothesis_test()` - Hypothesis test results with support indication

## Configuration

The `config.yml` file centralizes all parameters and file paths:

**Analysis Parameters:**
- `focal_industry_sic_code`: Target industry (default: 3571 - Electronic Computers)
- `focal_country`: Target country for focal firms (default: "United States")
- `min_date`: Minimum alliance date (default: "2009-12-31")

**Fuzzy Matching Parameters:**
- `fuzzy_match_threshold`: 0.85 (minimum score to accept match)
- `jw_weight`: 0.5 (Jaro-Winkler weight)
- `lv_weight`: 0.3 (Levenshtein weight)
- `jaccard_weight`: 0.2 (Jaccard weight)

**File Paths:**
- `raw_data_dir`: "data"
- `data_dir`: "working_data"
- All input/output file paths

## Dependencies

R packages required:
- **tidyverse** (dplyr, tidyr, purrr) - Data manipulation
- **igraph** - Network analysis
- **countrycode** - Geographic classification
- **stringdist** - Fuzzy matching (Jaro-Winkler, Levenshtein)
- **nstandr** - Company name standardization (Magerman method)
- **config** - Configuration management
- **readxl** - Excel file reading
- **janitor** - Column name cleaning
- **fs** - File system operations
- **crayon** - Console color output (reporting_utils.R)
- **knitr**, **kableExtra** - Table formatting (reporting_utils.R)

Install with:
```r
install.packages(c("tidyverse", "igraph", "countrycode",
                   "stringdist", "config", "readxl", "janitor",
                   "fs", "crayon", "knitr", "kableExtra"))
devtools::install_github("stasvlasov/nstandr")
```

## Required Data Files

### Input Files (in `data/` directory)
- `Export 17_12_2025 18_26.xlsx` - ORBIS company data (sheet: "Results")
- `SDC_data_2021.rds` - SDC Platinum alliance data
- `patents_view/*.csv` - Raw patent data files (multiple CSV files)

### Auxiliary Files
- `extra/sdc_to_orbis_map.csv` - Manual mapping between SDC and ORBIS company names

### Output Files (in `working_data/` directory)
All processed data files are automatically saved:
- `orbis_cleaned.csv` - Cleaned ORBIS firm data
- `filtered_sdc.csv` - Filtered strategic alliances
- `firm_patent_variables.csv` - Aggregated patent metrics by firm
- `sdc_patents_orbis_merged.csv` - Merged dataset (all sources)
- `analysis_sample.csv` - Final regression analysis sample
- `model_comparison.csv` - Model fit statistics
- `regression_models.rds` - Saved R model objects

## Execution Order

Run the R scripts in this sequence:

1. **`rds/patents_data.R`** - Process patent data
   - Input: `data/patents_view/*.csv`
   - Output: `working_data/firm_patent_variables.csv`

2. **`rds/sdc_data.R`** - Filter alliance data
   - Input: `data/SDC_data_2021.rds`
   - Output: `working_data/filtered_sdc.csv`

3. **`rds/orbis_data.R`** - Clean ORBIS data and apply mapping
   - Input: `data/Export 17_12_2025 18_26.xlsx`, `extra/sdc_to_orbis_map.csv`
   - Output: `working_data/orbis_cleaned.csv`

4. **`rds/merge_regression_dataframe.R`** - Merge all datasets
   - Input: All outputs from steps 1-3
   - Output: `working_data/sdc_patents_orbis_merged.csv`

5. **`rds/regression_analysis.R`** - Run network and regression analysis
   - Input: `working_data/sdc_patents_orbis_merged.csv`
   - Output: `working_data/analysis_sample.csv`, `model_comparison.csv`, `regression_models.rds`
