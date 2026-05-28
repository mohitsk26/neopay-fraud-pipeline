# 🛡️ NeoPay — Real-Time Fraud Detection Pipeline

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://neopay-fraud-pipeline-husaito9uzdpgumgjuwqn.streamlit.app)
[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://python.org)
[![Azure Databricks](https://img.shields.io/badge/Azure-Databricks-orange)](https://azure.microsoft.com/databricks)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.0-blue)](https://delta.io)
[![Apache Kafka](https://img.shields.io/badge/Apache-Kafka-black)](https://kafka.apache.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> A production-grade, end-to-end **real-time fraud detection pipeline** built on Azure.
> Synthetic payment transactions stream through Apache Kafka into a Medallion Architecture
> on Azure Databricks, get scored by a Random Forest ML model, forecasted by Facebook Prophet,
> and visualized in a live Streamlit dashboard — all in under 5 seconds per transaction.

---

## 🚀 Live Demo

| Resource | Link |
|---|---|
| 🌐 Live Streamlit App | [Launch Simulator](https://neopay-fraud-pipeline-husaito9uzdpgumgjuwqn.streamlit.app) |
| 💻 GitHub Repository | [View Code](https://github.com/mohitsk26/neopay-fraud-pipeline) |
| 🎬 Video Walkthrough | *Coming soon — Loom recording* |

---

## 📋 Table of Contents

- [Business Problem](#-business-problem)
- [System Architecture](#-system-architecture)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Key Concepts](#-key-concepts)
- [Step-by-Step Flow](#-step-by-step-flow)
- [Setup & Execution](#-setup--execution)
- [Databricks Notebooks](#-databricks-notebooks)
- [ML Model Details](#-ml-model-details)
- [Prophet Forecasting](#-prophet-forecasting)
- [Streamlit UI](#-streamlit-ui)
- [Security & DevSecOps](#-security--devsecops)
- [Resume Bullet Points](#-resume-bullet-points)

---

## 💼 Business Problem

**NeoPay** (fictional digital payments processor) handles 50,000+ transactions per minute across 12 countries.

**The Problem:** Legacy batch fraud scoring runs every 4 hours. By the time fraud is detected, chargebacks have already been filed — costing the business millions.

**The Solution:** A sub-5-second streaming fraud detection pipeline that:
- Scores every transaction in real time using ML
- Forecasts fraud rates 24 hours ahead using Prophet
- Gives fraud ops teams a live command center dashboard
- Processes data through governed Bronze/Silver/Gold layers

---

## 🏗️ System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     NEOPAY FRAUD PIPELINE                           │
└─────────────────────────────────────────────────────────────────────┘

  ┌──────────────────┐
  │  Streamlit UI    │  ← Hosted FREE on Streamlit Community Cloud
  │  (Simulator +    │    • Transaction Generator (configurable TPS)
  │   Dashboard)     │    • Fraud rate slider
  └────────┬─────────┘    • Prophet forecast chart
           │ kafka-python sends JSON events
           ▼
  ┌──────────────────┐
  │  Azure Event     │  ← Apache Kafka API
  │  Hubs (Kafka)    │    • Topic: transactions
  │                  │    • 4 partitions, Standard tier
  └────────┬─────────┘
           │ PySpark Structured Streaming
           ▼
  ┌──────────────────────────────────────────────────────┐
  │              Azure ADLS Gen2                         │
  │  bronze/transactions/   ← Raw JSON, append-only      │
  │  silver/transactions/   ← Cleaned + feature-eng.     │
  │  gold/fraud_scores/     ← ML-scored records          │
  │  gold/forecasts/        ← Prophet model outputs      │
  └────────┬─────────────────────────────────────────────┘
           ▼
  ┌──────────────────────────────────────────────────────┐
  │           Azure Databricks (PySpark)                 │
  │  Notebook 01: Bronze Ingest  (Streaming)             │
  │  Notebook 02: Silver Transform (Streaming)           │
  │  Notebook 03: ML Model Training (Batch, one-time)    │
  │  Notebook 04: Prophet Forecast (Scheduled hourly)    │
  └────────┬─────────────────────────────────────────────┘
           ▼
  ┌──────────────────┐
  │    Power BI      │  ← Business intelligence dashboard
  └──────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Event Streaming | Apache Kafka (Azure Event Hubs) | Real-time message queue |
| Cloud Storage | Azure ADLS Gen2 | Data lake with hierarchical namespace |
| Data Processing | Azure Databricks (PySpark) | Distributed stream processing |
| Table Format | Delta Lake | ACID transactions on data lake |
| Architecture | Medallion (Bronze/Silver/Gold) | Governed data layers |
| ML Framework | Scikit-learn (Random Forest) | Fraud classification |
| ML Tracking | MLflow + Unity Catalog | Model registry and versioning |
| Forecasting | Facebook Prophet | Time-series fraud rate prediction |
| UI / Simulator | Streamlit | Live demo and transaction simulator |
| Visualization | Plotly | Interactive forecast charts |
| BI Dashboard | Power BI | Business intelligence reporting |
| Auth / Security | Azure Service Principal, OAuth2 | Secure cloud access |
| Language | Python 3.11 | Primary development language |

---

## 📁 Project Structure

```
neopay-fraud-pipeline/
├── README.md
├── requirements.txt
├── .gitignore
├── ui/
│   ├── app.py                       ← Main Streamlit entry point
│   ├── mock_data/
│   │   └── sample_forecast.json     ← Fallback data for demo mode
│   └── .streamlit/
│       └── config.toml              ← UI theme (dark mode)
├── notebooks/
│   ├── 00_mount_adls.py             ← ADLS Gen2 authentication
│   ├── 01_bronze_ingest.py          ← Kafka → Bronze streaming
│   ├── 02_silver_transform.py       ← Bronze → Silver cleaning
│   ├── 03_train_model.py            ← ML model training + MLflow
│   └── 04_prophet_forecast.py       ← Prophet 24h forecasting
├── infrastructure/
│   └── azure/
│       └── deploy_resources.sh      ← Azure CLI provisioning
└── docs/
    └── architecture_diagram.png
```

---

## 🧠 Key Concepts

### Medallion Architecture
```
BRONZE → raw data, never modified, source of truth
SILVER → cleaned, parsed, feature-engineered
GOLD   → ML-scored, aggregated, BI-ready
```

### PySpark Structured Streaming
Processes data as a continuous stream with watermarking,
checkpointing, and exactly-once processing guarantees.

### Delta Lake
Brings ACID transactions to the data lake —
atomicity, consistency, isolation, and durability
on top of Parquet files in ADLS Gen2.

### MLflow Unity Catalog
Tracks every training run, logs metrics, and stores
versioned models with input/output signatures for
production deployment.

### Facebook Prophet
Meta's time-series forecasting library that captures
daily and weekly seasonality patterns to predict
future fraud rates with confidence intervals.

---

## 🔄 Step-by-Step Flow

```
T+0.0s  You click Start Stream in Streamlit
T+0.1s  Transaction JSON generated (amount, region, merchant, etc.)
T+0.2s  kafka-python sends to Azure Event Hubs (Kafka topic)
T+0.5s  Databricks Notebook 01 reads from Kafka → saves to Bronze
T+1.5s  Databricks Notebook 02 reads Bronze → cleans → saves to Silver
T+2.5s  ML model scores transaction → fraud_score saved to Gold
T+3.0s  Transaction appears in Streamlit feed with FRAUD/OK flag
T+60min Prophet forecast job runs → updates 24h prediction chart
T+any   Power BI refreshes → business dashboard updates
```

---

## ⚙️ Setup & Execution

### 1. Clone the Repository
```bash
git clone https://github.com/mohitsk26/neopay-fraud-pipeline.git
cd neopay-fraud-pipeline
```

### 2. Create Virtual Environment
```bash
# Windows
python -m venv venv
venv\Scripts\activate.bat

# Mac/Linux
python -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Run Streamlit Locally (Demo Mode — No Azure Needed)
```bash
python -m streamlit run ui/app.py
# Open: http://localhost:8501
# Click "Start Stream" to see live fraud detection
```

### 5. Configure Azure Credentials (Full Pipeline)
```bash
cp .env.example .env
# Edit .env with your Azure credentials
```

### 6. Run Databricks Notebooks (in order)
```
1. 00_mount_adls.py       → Run once
2. 03_train_model.py      → Run once
3. 04_prophet_forecast.py → Run once, then schedule hourly
4. 01_bronze_ingest.py    → Start streaming (leave running)
5. 02_silver_transform.py → Start streaming (leave running)
```

---

## 🤖 ML Model Details

**Algorithm:** Random Forest Classifier  
**AUC-ROC:** 0.7558  
**Training samples:** 8,000  
**Test samples:** 2,000  

| Feature | Description |
|---|---|
| amount | Transaction amount in USD |
| is_high_value | Amount > $1,000 |
| is_atm | ATM withdrawal transaction |
| is_off_hours | Between 10pm and 6am |
| hour_of_day | Hour of transaction (0-23) |

---

## 🔮 Prophet Forecasting

**Input:** 720 hours of historical fraud rates  
**Output:** 24-hour forecast with 95% confidence intervals  
**Seasonality:** Daily (night spikes) + Weekly (weekend patterns)  
**Avg predicted fraud rate:** ~3.01%

---

## 🔒 Security & DevSecOps

```
✅ Azure credentials    → .env file (gitignored)
✅ Kafka connection     → Streamlit secrets (not in code)
✅ Storage account key  → Databricks Spark config
✅ Service Principal    → Least-privilege RBAC
✅ TLS 1.2             → Event Hubs minimum
```

---

## 📈 Resume Bullet Points

> Architected production-grade real-time fraud detection pipeline
> ingesting transactions via Apache Kafka (Azure Event Hubs),
> processing through PySpark Structured Streaming Medallion
> Architecture on Azure Databricks with Delta Lake, reducing
> fraud detection latency from 4 hours to under 5 seconds.

> Engineered end-to-end ML scoring pipeline using Scikit-learn
> Random Forest (AUC-ROC: 0.76) with MLflow Model Registry on
> Databricks Unity Catalog, and Facebook Prophet forecasting
> generating 24-hour fraud rate predictions with 95% confidence.

> Delivered full-stack data product: live Streamlit simulator
> deployed on Streamlit Community Cloud with mock-data fallback
> and Power BI dashboard tracking real-time fraud KPIs and
> model accuracy for non-technical stakeholders.

---

## 🚀 Quick Start

```bash
git clone https://github.com/mohitsk26/neopay-fraud-pipeline.git
cd neopay-fraud-pipeline
pip install streamlit pandas plotly kafka-python
python -m streamlit run ui/app.py
```

---

## 📄 License

MIT License

---

*Built with Apache Kafka · Azure ADLS Gen2 · Azure Databricks ·
PySpark · Delta Lake · Prophet · MLflow · Streamlit*
