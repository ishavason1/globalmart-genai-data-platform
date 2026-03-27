# globalmart-genai-data-platform
End-to-end data engineering and GenAI use cases for the GlobalMart hackathon

GlobalMart – GenAI Data Platform
Overview
This repository contains an end‑to‑end data engineering and GenAI solution built for the GlobalMart hackathon.
The project demonstrates how scalable data pipelines using the Medallion architecture (Bronze, Silver, Gold) can be combined with LLM‑powered analytics and explanations to deliver trustworthy, auditable, and business‑ready insights.
All GenAI use cases are implemented on top of curated Gold layer tables and Materialized Views, ensuring that Large Language Models are used for interpretation and explanation, not raw data processing or decision‑making.

Architecture
The solution follows a layered, production‑aligned architecture:

Bronze Layer: Raw data ingestion
Silver Layer: Cleansed and standardized data
Gold Layer: Business‑ready tables, dimensional models, and Materialized Views
GenAI Layer: LLM‑driven explanations, retrieval‑augmented Q&A, and executive summaries

The architecture intentionally separates:

Deterministic logic (rules, aggregations, KPIs)
From probabilistic inference (LLM explanations and summaries)

Refer to the diagrams in the architecture/ folder for detailed data flow and component interactions.

Data Engineering
Medallion Layers

Bronze: Ingests source data as‑is
Silver: Applies data cleansing, normalization, and enrichment
Gold: Exposes analytical tables, Materialized Views, and dimensional models for reporting and AI use cases

Materialized Views
Gold‑layer Materialized Views compute key business metrics such as:

Revenue by region
Vendor return rates
Slow‑moving inventory indicators
Product and vendor performance metrics

These views serve as the single source of truth for downstream analytics and GenAI use cases.
Dimensional Model
A star‑schema–style dimensional model is used to support analytical workloads, with fact tables and dimensions for products, vendors, regions, and time.
Details are documented in the data-model/ folder.

GenAI Use Cases
UC1: Data Quality Explanation
Automatically translates failed data quality checks into clear, human‑readable explanations.
The LLM summarizes root causes, business impact, and recommended next steps to help stakeholders quickly understand data issues.
UC2: Fraud Detection and Explanation
Fraud detection is performed using rule‑based scoring for consistency and auditability.
The LLM is used only to generate explanations describing why transactions were flagged, referencing the underlying rules and metrics.
UC3: Retrieval‑Augmented Product & Vendor Q&A (RAG)
Implements a RAG‑based question‑answering system over product and vendor data:

Gold table rows are converted into natural‑language documents
Documents are embedded locally using sentence‑transformers (all‑MiniLM‑L6‑v2)
A FAISS index is used for semantic retrieval
The LLM answers questions strictly from retrieved documents and explicitly states when answers are not found
All queries and responses are logged to globalmart.gold.rag_query_history

Example queries include slow‑moving products, vendor return rate comparisons, and region‑specific availability.
UC4: AI‑Generated Business Insights
Reads aggregated KPI data from Gold Materialized Views and generates executive‑level summaries for:

Revenue performance by region
Vendor return rates
Slow‑moving inventory

Results are written to globalmart.gold.ai_business_insights, including:

Insight type
AI‑generated executive summary
KPI data passed to the LLM (as JSON, for auditability)
Generation timestamp

This use case also demonstrates ai_query(), Databricks’ SQL‑native function for invoking foundation models directly from SQL.

Repository Structure
.
├── README.md
├── architecture/          # Architecture and data flow diagrams
├── data-model/            # Schemas and dimensional model documentation
├── pipelines/             # Bronze, Silver, Gold pipeline code
├── notebooks/             # UC1–UC4 executable notebooks
├── sql/                   # Materialized Views and ai_query examples
└── docs/                  # Team summary and design documentation


How to Run

Clone the repository
Import the notebooks into a Databricks workspace
Ensure all referenced Bronze, Silver, and Gold tables exist
Run notebooks top to bottom in the notebooks/ folder

All notebooks are designed to run end‑to‑end without manual intervention.

Outputs
The solution produces and/or populates the following Gold‑layer tables:

globalmart.gold.rag_query_history
globalmart.gold.ai_business_insights

These tables provide full traceability of GenAI interactions and outputs.

Design Principles

Deterministic first: Rules and aggregations handle decisions and metrics
LLMs for interpretation: Used for explanation, summarization, and Q&A
Auditability: KPI inputs and model outputs are persisted
Separation of concerns: Data engineering and GenAI responsibilities are clearly decoupled


Team
This project was developed as part of a team effort for the GlobalMart hackathon.
Individual contributions and design decisions are documented in docs/.

