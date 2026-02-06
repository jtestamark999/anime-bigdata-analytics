## Anime Big Data Analytics System

## Overview
This project designs and implements a Big Data pipeline for analyzing large-scale anime viewing data and metadata. The system collects, processes, stores, and analyzes anime-related datasets using distributed data processing concepts.

The project is developed for CS 4265 — Big Data Analytics: Building Data Pipelines at Scale.

---

## Objectives
- Collect anime data from public datasets and APIs
- Clean and validate raw data
- Store structured data in a NoSQL database
- Perform large-scale analysis using Apache Spark
- Generate analytical reports and visualizations

---

## Technology Stack

| Layer       | Technology              |
|-------------|--------------------------|
| Storage     | Local File System / HDFS (Conceptual) |
| Processing  | Apache Spark (PySpark)   |
| Data Store  | MongoDB                  |
| Querying    | Spark SQL                |
| Formats     | JSON, CSV, Parquet        |

---

## System Architecture
The system follows a layered Big Data architecture:

1. Data is collected from external APIs and datasets
2. Raw data is cleaned and transformed using Python
3. Processed data is stored in MongoDB
4. Apache Spark performs distributed analysis
5. Results are presented to users as reports

Architecture diagrams are available in the `/docs` directory.

---

## Project Structure

