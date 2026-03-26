# Medicare Provider Intelligence Hub
![Project Architecture Diagram](architecture_w.png)
### 🏥 Project Overview
In the modern healthcare landscape, identifying cost outliers and provider inefficiencies is a multi-billion dollar challenge. This project implements a robust, end-to-end **Batch ETL Pipeline** designed to ingest, process, and analyze the **CMS Medicare Synthetic Public Use Files (SynPUF)**. 

By leveraging a **Medallion Architecture** (Bronze, Silver, Gold), this platform transforms raw, cryptic medical claims into high-fidelity analytical assets that power executive decision-making.

---

### 🚀 Key Features
* **Infrastructure as Code (IaC):** Fully reproducible cloud environment using **Terraform** to provision GCS buckets and BigQuery datasets.
* **Medallion Data Architecture:** * **Bronze:** Raw landing of CMS files in Parquet/CSV format.
    * **Silver:** Cleaned and standardized staging tables (renaming obscure CMS headers to business logic).
    * **Gold:** Production-ready Fact and Dimension tables for provider performance and fraud detection.
* **Automated Orchestration:** A scheduled workflow managed by **Apache Airflow / Mage**, handling extraction, schema enforcement, and transformation triggers.
* **Analytics Engineering:** Complex SQL transformations and data modeling using **dbt**, ensuring data quality with built-in testing.

---

### 🛠️ Tech Stack
| Category | Tools |
| :--- | :--- |
| **Cloud Provider** | Google Cloud Platform (GCP) |
| **Infrastructure** | Terraform |
| **Orchestration** | Apache Airflow / Mage |
| **Data Lake** | Google Cloud Storage (GCS) |
| **Data Warehouse** | BigQuery |
| **Transformation** | dbt (data build tool) |
| **Visualization** | Looker Studio |
| **Language** | Python, SQL |

---

### 📐 Architecture
*(Insert your Excalidraw image here)*

The pipeline follows a modern ELT pattern:
1.  **Extract:** Python-based ingestion from CMS sources.
2.  **Load:** Data is landed in **GCS (Bronze Zone)** and registered as **External Tables in BigQuery**.
3.  **Transform:** **dbt** performs heavy lifting inside BigQuery to create the **Silver (Staging)** and **Gold (Marts)** layers.
4.  **Analyze:** Final dimensions and facts are surfaced in **Looker Studio** for KPI tracking.

---

### 📊 Analytical Goals
* **Provider Benchmarking:** Identifying providers with significantly higher reimbursement rates for standardized procedures.
* **Chronic Condition Correlation:** Mapping patient demographics to claim costs to identify high-risk segments.
* **Geographic Cost Analysis:** Heatmaps showing healthcare spending density across US states.

---

### 🛠️ Setup & Usage
*(This section will be filled in as you finalize your code, but here is a placeholder)*
1. Clone the repo.
2. Navigate to `/terraform` and run `terraform apply`.
3. Configure your GCP credentials in the `/orchestration` layer.
4. Trigger the DAG to populate your warehouse.
