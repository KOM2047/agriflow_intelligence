# 🌾 AgriFlow Intelligence: Automated ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.12-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-2.7.1-017CEE)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED)
# 🌾 AgriFlow Intelligence: Automated ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.12-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-2.7.1-017CEE)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED)
![Pandas](https://img.shields.io/badge/Pandas-Data_Manipulation-150458)

---

## Project Overview
AgriFlow Intelligence is a fully containerized **Data Engineering pipeline** designed to process agricultural supply chain and market data.  
The system automatically ingests, transforms, and loads **daily farm harvest logs** and **external market price feeds** into a PostgreSQL Data Warehouse, enabling accurate **profit margin analysis** and **business intelligence reporting**.

---

## Architecture

The pipeline follows a robust **ETL architecture** orchestrated via **Apache Airflow**:

1. **Extraction (Python Generators & Bash)**  
   - Simulates internal business systems (CSV harvest logs) and external API endpoints (JSON market prices).  
   - Bash scripts validate file integrity and move raw data into a staging environment.  

2. **Transformation (Python / Pandas)**  
   - Cleans missing values and enforces data quality rules.  
   - Merges internal crop yields with external market prices.  
   - Applies business logic to calculate **labor costs, logistics costs, gross revenue, and net profit**.  

3. **Loading (PostgreSQL)**  
   - Dimension lookups map business keys to surrogate keys.  
   - Enforces **idempotency** (`DELETE-before-INSERT`) to prevent duplicate records on re-runs.  

4. **Orchestration (Airflow)**  
   - Automates the workflow as a **daily scheduled DAG**.  

### System Diagram
Below is the architecture diagram (Mermaid). On GitHub the diagram will render automatically. If your viewer doesn't support Mermaid, consider generating the SVG in `docs/`.

```mermaid
graph TD
    %% Define Styles
    classDef storage fill:#f9f,stroke:#333,stroke-width:2px;
    classDef compute fill:#bbf,stroke:#333,stroke-width:1px,stroke-dasharray: 5 5;
    classDef orchestrator fill:#ff9,stroke:#333,stroke-width:2px;
    classDef external fill:#ddd,stroke:#333,stroke-width:1px;

    %% Orchestration Layer
    subgraph Orchestration [Orchestration Layer (Docker Container)]
        Airflow((Apache Airflow <br/> Scheduler & Webserver)):::orchestrator
        MetaDB[(Airflow Metadata <br/> Postgres DB)]:::storage
        Airflow -.->|Tracks State| MetaDB
    end

    %% Source Systems (Simulated)
    subgraph Sources [Source Layer (Logical)]
        FarmLogs[Internal Farm <br/> Management System]:::external
        MarketAPI[External Market <br/> Price API Feed]:::external
    end

    %% Data Lake / File Storage (Mounted Volumes)
    subgraph DataLake [Data Lake / Landing Zone (Local Volumes)]
        RawZone[data/raw <br/> Landing Area]:::storage
        StagingZone[data/staging <br/> Validated Area]:::storage
    end

    %% Compute / ETL Layer
    subgraph Compute [ETL & Transformation Layer (Docker Container)]
        GenScripts(Python Generators <br/> generate_harvests.py <br/> mock_market_api.py):::compute
        IngestScript(Bash Script <br/> ingest.sh + sed):::compute
        ETLScript(Python/Pandas <br/> extract.py <br/> transform.py):::compute
    end

    %% Data Warehouse
    subgraph Warehouse [Data Warehouse (Docker Container)]
        PostgresDW(((PostgreSQL <br/> agriflow_dw)) <br/> Star Schema Mapping):::storage
        pgAdmin[pgAdmin4 <br/> UI Interface]:::compute
    end

    %% ---------------------------------------------------------
    %% Define Flows
    %% ---------------------------------------------------------
    
    %% 1. Airflow Control Flow (Dashed Lines)
    Airflow ==>|1. Triggers| GenScripts
    Airflow ==>|2. Triggers| IngestScript
    Airflow ==>|3. Executes| ETLScript

    %% 2. Data Flow (Solid Lines)
    FarmLogs -->|Simulated CSV| GenScripts
    MarketAPI -->|Simulated JSON| GenScripts
    
    GenScripts -->|Writes Raw Files| RawZone
    RawZone -->|Reads Raw| IngestScript
    
    IngestScript -->|Moves & Cleans (LF)| StagingZone
    StagingZone -->|Reads Staging| ETLScript
    
    ETLScript -->|Load (Idempotent)| PostgresDW
    
    pgAdmin -.->|Manages/Queries| PostgresDW
```

---

## Data Modeling (Star Schema)

The Data Warehouse is designed using **dimensional modeling** in PostgreSQL, optimized for OLAP queries and BI dashboards.

- **Fact Table:**  
  `fact_harvest_yield` → Metrics: `quantity_harvested_kg`, `spoilage_kg`, `revenue_zar`, `profit_zar`  

- **Dimension Tables:**  
  - `dim_crop` → Crop types and varieties  
  - `dim_farm` → Farm locations, managers, Type 2 SCD tracking  
  - `dim_date` → Time-series analysis  

---

## Quick Start (Local Execution)

This project is **100% containerized**. You only need Docker installed to run the environment.

### 1. Clone the repository
```bash
git clone https://github.com/KOM2047/agriflow_intelligence.git
cd agriflow_intelligence
```

### 2. Build and start containers
```bash
docker-compose up --build
```

### 3. Access Airflow UI
- Navigate to: `http://localhost:8080`  
- Default credentials: `airflow / airflow`

### 4. Run the pipeline
- Trigger the DAG: `agriflow_etl_pipeline`  
- Monitor execution via Airflow UI  

---

## Example Output

Once executed, the pipeline produces analytical tables in PostgreSQL. Example query:

```sql
SELECT 
    f.crop_id, 
    f.farm_id, 
    d.date, 
    SUM(f.quantity_harvested_kg) AS total_yield, 
    SUM(f.profit_zar) AS total_profit
FROM fact_harvest_yield f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY f.crop_id, f.farm_id, d.date;
```

---

## Tech Stack

- **Languages:** Python (3.12), Bash  
- **Libraries:** Pandas  
- **Database:** PostgreSQL (15)  
- **Orchestration:** Apache Airflow (2.7.1)  
- **Containerization:** Docker  

---

## Future Enhancements

- Integration with **real-time IoT farm sensors**  
- **Data quality monitoring** with Great Expectations  
- **BI dashboards** via Power BI / Metabase  
- **Cloud deployment** (AWS / Azure)  

---

## Author

**Kabelo Modimoeng**  
Emerging Cloud & AI Engineer | Data Engineering & Workflow Automation  
Sandton Tech Market | LinkedIn [(linkedin.com in Bing)](https://www.bing.com/search?q="https%3A%2F%2Fwww.linkedin.com%2Fin%2Fkabelo-modimoeng")  

---

## Contributing

Contributions are welcome!  
Fork the repo, create a feature branch, and submit a PR.  

---

## License

This project is licensed under the MIT License.
