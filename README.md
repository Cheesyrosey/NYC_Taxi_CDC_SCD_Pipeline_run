🚖 NYC Taxi CDC → SCD Type 2 Pipeline
=====================================

### A Production-Style Databricks Medallion Project

1\. Introduction ✨
------------------
### 🧠 Human-friendly summary

Imagine you run a taxi company. You have trip record that consists of the time your driver picks up the passenger and the time of drop off, together with the fare amount and the distance, ect. However, the server keeps sending updates on the drive. Every time a trip record changes — maybe the fare was corrected or the pickup ZIP code was updated — you want to:

*   Keep the old version
    
*   Store the new version
    
*   Know exactly when each version was valid
    
*   Make sure nothing breaks along the way
  
These describes a typical SCD Type 2 objectives. The project hits these objectives by building a **full production-style data pipeline on Databricks** that:

*   Loads real **NYC Taxi** data
    
*   Simulates **Change Data Capture (CDC)** — what changed since last time
    
*   Processes data through **Bronze → Silver → Gold** layers
    
*   Maintains a **full history of every change using SCD Type 2**
    
*   Runs **automated data quality tests**
    
*   Orchestrates everything with a **multi-task Databricks Job Flow**
    
2\. Architecture 🏗️
--------------------

```text
Raw (Base Fact)
  ↓
Bronze (CDC events: before/after)
  ↓
Silver (flattened CDC snapshots)
  ↓
Gold (SCD Type 2 history table)
  ↓
Tests (quality gate)
```
### 📊 Pipeline Flow (Mermaid Diagram)

flowchart TD

    A[01 Base Fact] --> B[02 Bronze CDC Generator]
    B --> C[03 Silver Normalization]
    C --> D[04 Gold SCD Type 2 Merge]
    D --> E[05 Unit Tests]

    E --> B2[06 Bronze CDC Batch 2]
    B2 --> C2[07 Silver Batch 2]
    C2 --> D2[08 Gold Merge 2]
    D2 --> E2[09 Tests Batch 2]

    E2 --> B3[10 Bronze CDC Batch 3]
    B3 --> C3[11 Silver Batch 3]
    C3 --> D3[12 Gold Merge 3]
    D3 --> E3[13 Tests Batch 3]

### 🧠 Human-friendly summary

Think of this like a factory assembly line:

*   **Bronze** = raw ingredients: CDC event
    
*   **Silver** = cleaned and prepared: Flattened CDC 'after' data
    
*   **Gold** = final packaged product: Upserted history table
    
*   **Tests** = quality control: Unit tests to ensure everything works correctly
    

Each CDC batch goes through the same steps — just like a real production pipeline.

3\. Technology Used ⚙️
----------------------

### ✅ Used in this project

*   Databricks Lakehouse
    
*   Delta Lake
    
*   PySpark
    
*   SQL
    
*   LakeFlow Jobs
    
*   Widgets & Parameters
    
*   OPTIMIZE + ZORDER

4\. Skills Demonstrated 🎯
--------------------------

This project showcases **real data engineering capabilities**:

*   ✔️Designing **production-style** data pipelines <br>
    Built a multi‑layer Bronze → Silver → Gold pipeline that mirrors real enterprise medallion architecture.
*   ✔️Implementing **CDC ingestion** patterns <br>
    Simulated multi‑batch CDC updates (before/after rows) to mimic how real systems capture data changes.
*   ✔️Building **SCD Type 2** history tables <br>
    Maintained full change history using valid_from, valid_to, and is_current fields for every entity.
*   ✔️Writing **idempotent Delta MERGE** logic <br>
    Used deterministic MERGE patterns so rerunning a batch produces the same correct result every time.
*   ✔️**Debugging** complex data flows <br>
    Traced issues across Bronze → Silver → Gold using time travel, schema checks, prevention of duplicates, and event timestamp comparisons.
*   ✔️Creating **parameterized, multi-batch workflows** <br>
    Used Databricks widgets (dbutils.widgets.get("batch_id")) to run the same pipeline across multiple CDC batches.
*   ✔️Writing **automated data quality tests** <br>
    Added schema validation, CDC correctness checks, and SCD Type 2 integrity tests as Job quality gates.
*   ✔️**Optimizing** Delta tables <br>
    Applied OPTIMIZE and ZORDER to reduce file fragmentation and improve query performance.
*   ✔️Understanding **orchestration & job dependencies** <br>
    Built a 13‑task Databricks Job Flow with strict upstream/downstream dependencies and failure alerts.
### 🧠 Human-friendly summary
This isn’t just SQL + Pyspark. It shows the ability to **design, build, test, and operate real data pipelines** that safely handle **evolving** data.

### 🌩️Technologies I could use in a real cloud environment

_(Not used here to avoid cloud costs — but included to demonstrate architectural awareness.)_

* **Delta Live Tables (DLT) Delta Live Tables (DLT)** <br>
Common practice: Used for declarative, continuously running pipelines with built‑in data quality rules.
In this project: I used standard notebooks + Jobs instead of DLT to keep the setup lightweight and cost‑friendly.
* **Streaming CDC (Auto Loader / Kafka)** <br>
Common practice: Real‑time ingestion of CDC events from cloud storage or message queues.
In this project: I simulated CDC in batches to mimic real behavior without streaming infrastructure.
* **Unity Catalog + Lineage** <br>
Common practice: Centralized governance, permissions, and automatic lineage tracking across all tables.
In this project: I used regular Databricks tables, but the pipeline structure is fully compatible with Unity Catalog.
* **CI/CD with GitHub Actions** <br>
Common practice: Automated testing, notebook promotion, and deployment across dev/stage/prod environments.
In this project: I did not use CI/CD, but the modular notebook structure is CI/CD‑friendly.
* **Feature Store / MLflow for downstream ML** <br>
Common practice: Gold tables feed ML pipelines, feature stores, and model training workflows.
In this project: I focused on data engineering, but the SCD Type 2 Gold table is ML‑ready.
* **Monitoring & Alerting** <br>
Common practice: Pipelines send alerts, logs, and metrics to detect failures quickly.
In this project: I added a Databricks Job email alert for failure notifications — a lightweight but realistic monitoring step.

### 🧠 Human-friendly summary 
This project uses the **same tools real companies use in production**. It also demonstrates awareness of the broader cloud data ecosystem — even when those tools aren’t directly used.


5\. Notebook Overview 📚
------------------------

### 01\_base\_fact\_trips

Loads the initial NYC Taxi dataset and prepares a clean **base fact table**.

### 02\_bronze\_cdc\_generator

Simulates realistic **CDC events** across multiple batches using a *batch\_id* parameter.

### 03\_silver\_normalization

Flattens Bronze CDC into clean, business-ready rows(one row per **after** state).

### 04\_gold\_scd\_merge

Applies full **SCD Type 2** logic:

*   Closes old versions
    
*   Inserts new versions
    
*   Maintains valid\_from, valid\_to, is\_current
    
*   Guarantees **exactly one current row per entity**
    

### 05\_unit\_tests

Runs automated checks:

*   Schema validation
    
*   CDC correctness
    
*   SCD Type 2 rules
    
*   Version growth validation
    

🚫 If **any test fails → the job stops**

### 🧠 Human-friendly summary

Each notebook represents **one step in the assembly line** — from raw data to clean historical truth, with quality checks at the end.

6\. Extras ✨
------------
© 2025 Joy — This project is created for learning and portfolio demonstration.  
If you reuse or adapt any part of this work, please provide proper attribution.

I’m Joy — a data analyst/data scientist moving into data engineering with a strong focus on real‑world pipeline design.  
I enjoy turning messy, evolving data into reliable, automated systems using Databricks, Delta Lake, and PySpark.
My projects emphasize engineering maturity: automation, testing, orchestration, and clarity.
    

