## Anime Big Data Analytics System

## Overview
This project designs and implements a Big Data pipeline for analyzing large-scale anime viewing data and metadata. The system collects, processes, stores, and analyzes anime-related datasets using distributed data processing concepts.

---

## Objectives
- Collect anime data from public datasets and APIs
- Clean and validate raw data
- Store structured data in a NoSQL database
- Perform large-scale analysis using Apache Spark
- Generate analytical reports and visualizations

---

## Technology Stack         

 Storage - Local File System / HDFS (Conceptual) 
 Processing - Apache Spark (PySpark)   
Data Store - MongoDB                  
 Querying - Spark SQL                
 Formats - JSON, CSV          


## System Architecture
The system follows a layered Big Data architecture:

1. Data is collected from external APIs and datasets
2. Raw data is cleaned and transformed using Python
3. Processed data is stored in MongoDB
4. Apache Spark performs distributed analysis
5. Results are presented to users as reports

Architecture diagrams are available in the `/docs` directory.

---

## Setup and Installation
Prerequisites
Ensure you have Python 3.x installed. Install the necessary libraries using the command:
pip install -r requirements.txt

Prepare the CSV

Download the Kaggle dataset (anime-dataset-2023.csv).

Place it inside the "data/raw" directory.

How to Run the Pipeline
To execute the full Milestone 2 data flow, run the scripts in this specific order using your terminal or command prompt:

Step 1. Ingest API Data:

Run: python src/fetch_anilist.py

Run: python src/fetch_jikan.py

Step 2. Merge Raw Files:

Run: python src/data_merger.py

Step 3. Verify and Test:

Run: python src/main.py

(The main script will display the first 20 records—including Name, Rating, and Genres—from all three sources to verify the pipeline is working correctly.)

--- 

## Milestone 2 Status and Progress
Done Data Acquisition: Successfully fetching records via GraphQL and REST.

Done Local Storage: Implemented structured raw directory.

Done Verification: main.py successfully displays uniform metadata across sources.

Planned Data Cleaning: Fuzzy matching and deduplication (For M3).

Planned Storage: Loading processed JSON into MongoDB (For M3).

Planned Analytics: Complex Spark SQL queries for popularity trends (For M3).

