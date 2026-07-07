# DataBricks Formula 1 Project

## Overview

This is a comprehensive data engineering project built on **Azure Databricks** that processes and analyzes Formula 1 World Championship data from 1950 to 2020. The project implements a **medallion architecture** (bronze/raw → silver/processed → gold/presentation) to organize data, with Apache Spark for ETL and Delta Lake for data storage.

The project includes data ingestion pipelines, transformations, analysis queries, and visualization dashboards using Power BI and Databricks SQL.

---

## Project Architecture

### Medallion Architecture Layers

```
Raw Layer (Bronze)
    ↓
Processed Layer (Silver)
    ↓
Presentation Layer (Gold)
```

#### Layer Descriptions:

- **Raw Layer (Bronze)**: Contains raw data from Kaggle Formula 1 dataset
  - Location: `/mnt/formula1dlgen2/raw/`
  - Format: CSV/JSON files

- **Processed Layer (Silver)**: Cleaned, validated, and transformed data
  - Location: `/mnt/formula1dlgen2/processed/`
  - Format: Delta tables with schema enforcement
  - Database: `f1_processed`

- **Presentation Layer (Gold)**: Aggregated data for analysis and visualization
  - Location: `/mnt/formula1dlgen2/presentation/`
  - Database: `f1_presentation`

---

## Data Sources

### Primary Dataset
- **Kaggle Formula 1 World Championship Dataset (1950-2020)**
  - Source: https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020
  - Contains complete F1 historical data including races, drivers, constructors, and results

### Database Schema Reference
- **ERGast Database Schema**: http://ergast.com/images/ergast_db.png
- Contains comprehensive relational schema for F1 data

---

## Project Structure

```
DataBricks_Formula1_Project/
├── formula1-project/
│   ├── formula1-project/
│   │   ├── set-up/                    # Setup and configuration
│   │   │   └── service principal.py   # Azure service principal setup
│   │   │
│   │   ├── Ingestion/                 # Data ingestion notebooks
│   │   │   ├── 0.create-processed_database.sql
│   │   │   ├── 1.ingest_circuits_file.py
│   │   │   ├── 2.ingest_races_file.py
│   │   │   ├── 3.ingest constructors.json file.py
│   │   │   ├── 4.ingest drivers.json file.py
│   │   │   ├── 5.ingest lap_times_file.py
│   │   │   ├── 6.ingest pit_stop.json file.py
│   │   │   ├── 7.ingest results.json file.py
│   │   │   └── 8.ingest_qualifying_file.py
│   │   │
│   │   ├── transformations/            # Data transformation notebooks
│   │   │   ├── 0.create_presentation_database.sql
│   │   │   ├── 1.race_results.py
│   │   │   ├── 2.driver_standings.py
│   │   │   ├── 3.constructor_standing.py
│   │   │   └── 4.calculated_race_results.sql
│   │   │
│   │   ├── analysis/                   # Analysis and visualization queries
│   │   │   ├── 0.find_dominant_drivers.sql
│   │   │   ├── 2.find_dominant_teams.sql
│   │   │   ├── 3.vis_dominant_drivers.sql
│   │   │   └── 4.viz_dominant_teams.sql
│   │   │
│   │   └── includes/                   # Shared configuration files
│   │       └── configuration           # Common variables and paths
│   │
│   └── manifest.mf                     # Project manifest
│
├── Data Lake Gen2 Storage/             # Azure Data Lake documentation
├── DataBricks Dashbroad/               # Databricks dashboard exports
├── Final Tables/                       # Final analysis tables
├── Kaggle Data(raw)/                   # Raw Kaggle dataset
│
├── Compute Cluster Configurations.png  # Cluster setup documentation
├── database schema.png                 # Visual representation of DB schema
├── PowerBI Report.pbix                 # Power BI visualization report
├── solution.png                        # Architecture diagram
├── sql data.png                        # Sample SQL query results
├── formula1-project.dbc                # Databricks archive/backup
└── info.txt                            # Project references

```

---

## Key Components

### 1. Setup and Configuration

**File**: `formula1-project/set-up/service principal.py`

Configures Azure authentication for Databricks:
- Sets up OAuth authentication with Azure AD
- Defines service principal credentials
- Mounts Azure Data Lake Gen2 storage containers:
  - `/mnt/formula1dlgen2/demo` - Demo data
  - `/mnt/formula1dlgen2/raw` - Raw ingestion data
  - `/mnt/formula1dlgen2/processed` - Silver layer data
  - `/mnt/formula1dlgen2/presentation` - Gold layer data

### 2. Data Ingestion Pipeline

Ingests raw F1 data from CSV/JSON files into Delta tables:

| Notebook | Source | Table | Description |
|----------|--------|-------|-------------|
| `1.ingest_circuits_file.py` | circuits.csv | `f1_processed.circuits` | F1 race circuits/venues |
| `2.ingest_races_file.py` | races.csv | `f1_processed.races` | Race events and dates |
| `3.ingest constructors.json file.py` | constructors.json | `f1_processed.constructors` | F1 teams/constructors |
| `4.ingest drivers.json file.py` | drivers.json | `f1_processed.drivers` | Driver information |
| `5.ingest lap_times_file.py` | lap_times.csv | `f1_processed.lap_times` | Lap timing data |
| `6.ingest pit_stop.json file.py` | pit_stops.json | `f1_processed.pit_stops` | Pit stop data |
| `7.ingest results.json file.py` | results.json | `f1_processed.results` | Race results and points |
| `8.ingest_qualifying_file.py` | qualifying.csv | `f1_processed.qualifying` | Qualifying session results |

**Key Ingestion Features**:
- Schema validation using PySpark StructType definitions
- Column renaming for consistency
- Addition of `ingestion_date` timestamp
- Delta format for ACID transactions and versioning
- Data stored in Parquet optimized format

### 3. Data Transformations

**Database**: `f1_presentation`

Transforms silver-layer data into analytical tables:

| Notebook | Purpose | Output |
|----------|---------|--------|
| `1.race_results.py` | Transforms results with driver/constructor/circuit info | Clean race results with context |
| `2.driver_standings.py` | Calculates cumulative driver championship standings | Driver points progression |
| `3.constructor_standing.py` | Calculates cumulative constructor standings | Team championship standings |
| `4.calculated_race_results.sql` | Advanced race-level calculations | Enhanced race metrics |

### 4. Analysis and Insights

**Location**: `formula1-project/analysis/`

SQL-based analytical queries:

- **`0.find_dominant_drivers.sql`**: Identifies top-performing drivers with statistics
- **`2.find_dominant_teams.sql`**: Identifies top-performing constructors
- **`3.vis_dominant_drivers.sql`**: Visualization query for driver performance trends
- **`4.viz_dominant_teams.sql`**: Visualization query for constructor performance trends

---

## Technologies and Tools

### Core Technologies
- **Apache Spark 3.x** - Distributed data processing
- **Delta Lake** - ACID transactions and data versioning
- **PySpark** - Python API for Spark
- **SQL** - Data transformation and analysis

### Cloud Infrastructure
- **Azure Databricks** - Managed Spark clusters
- **Azure Data Lake Gen2 (ADLS Gen2)** - Data storage (ABFSS protocol)
- **Azure AD / Service Principal** - Authentication and authorization

### BI & Visualization
- **Power BI** - Business intelligence dashboards (PowerBI Report.pbix)
- **Databricks SQL** - Interactive SQL workbench
- **Databricks Dashboards** - Native Databricks visualizations

---

## Getting Started

### Prerequisites
1. Azure Subscription with:
   - Databricks workspace provisioned
   - Data Lake Gen2 storage account
   - Service Principal with appropriate permissions

2. Formula 1 dataset from Kaggle

### Setup Steps

1. **Create Service Principal**
   - Execute `formula1-project/set-up/service principal.py`
   - Configure your Azure credentials (client_id, tenant_id, client_secret)
   - Mount all required storage containers

2. **Create Databases**
   - Run `0.create-processed_database.sql` to create `f1_processed` database
   - Run `transformations/0.create_presentation_database.sql` to create `f1_presentation`

3. **Run Ingestion Pipeline**
   - Execute ingestion notebooks (1-8) in sequence
   - Verifies data is loaded into `f1_processed` tables

4. **Run Transformations**
   - Execute transformation notebooks (1-4)
   - Creates gold-layer tables in `f1_presentation`

5. **Run Analysis Queries**
   - Execute analysis SQL files to generate insights
   - Connect Power BI to Databricks SQL for visualization

---

## Key Features

✅ **Medallion Architecture**: Clean separation of raw, processed, and presentation layers  
✅ **Delta Lake**: ACID transactions, time travel, and schema enforcement  
✅ **Scalable ETL**: PySpark for distributed processing of large datasets  
✅ **Version Control**: Full data lineage and change tracking  
✅ **Production Ready**: Error handling and data quality checks  
✅ **Cloud Native**: Fully leverages Azure cloud services  
✅ **Interactive Analytics**: SQL workbench + BI dashboards  

---

## Data Quality & Transformations

### Ingestion Transformations
- Schema validation with strict type definitions
- Column standardization (snake_case naming)
- Timestamp addition for audit trails
- Duplicate handling (overwrite mode)

### Analytics Transformations
- Driver/Constructor standings calculations
- Race result aggregations
- Performance metrics computation
- Temporal trend analysis

---

## Visualization & Reporting

### Power BI Dashboard
- File: `PowerBI Report.pbix`
- Connected to Databricks SQL endpoint
- Includes visualizations for:
  - Dominant drivers over seasons
  - Dominant teams/constructors
  - Race statistics and trends

### Databricks Dashboards
- Location: `DataBricks Dashbroad/`
- Interactive SQL dashboards
- Real-time data exploration

---

## Project Deliverables

- ✅ **SQL Database Schema**: `database schema.png`
- ✅ **Architecture Diagram**: `solution.png`
- ✅ **Cluster Configuration**: `Compute Cluster Configurations.png`
- ✅ **BI Reports**: `PowerBI Report.pbix`
- ✅ **Sample Outputs**: `sql data.png`
- ✅ **Databricks Archive**: `formula1-project.dbc`

---

## References

- **ERGast Formula 1 Database**: http://ergast.com/
- **Kaggle Dataset**: https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020
- **Databricks Documentation**: https://docs.databricks.com/
- **Delta Lake**: https://delta.io/

---

## Author

**PalkishAkula**

---

## License

This project is provided as-is for educational and analytical purposes.

---

## Notes

- All notebooks are Databricks notebooks (`.py` with Databricks magic commands)
- `.dbc` file is an archive of the entire project for backup/sharing
- Configuration variables are centralized in `includes/configuration`
- All data is processed using Delta Lake format for reliability

