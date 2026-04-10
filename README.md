## Anime Big Data Analytics System

## Overview
This project designs and implements a Big Data pipeline to analyze large-scale anime-viewing data and metadata. The system collects, processes, stores, and analyzes anime-related datasets using distributed data processing concepts.

---

## Objectives
- Collect anime data from public datasets and APIs
- Clean and validate raw data
- Store structured data in a NoSQL database
- Perform large-scale analysis using Apache Spark
- Generate analytical reports 

---

## Technology Stack         

 Storage - Local File System / HDFS (Conceptual) 
 Processing - Apache Spark (PySpark)   
Data Store -parquet                 
 Querying - Spark SQL                
 Formats - JSON, CSV          


## System Architecture
The system follows a layered Big Data architecture:

1. Data is collected from external APIs and datasets
2. Raw data is cleaned and transformed using Python
3. Processed data is stored in parquet
4. Apache Spark performs distributed analysis
5. Results are presented to users as reports

Architecture diagrams are available in the `/docs` directory.

---

## Setup and Installation
### 1. Installation & Setup

First, ensure you have **Apache Spark** installed and your environment variables (like `SPARK_HOME`) properly configured. Then, install the necessary Python dependencies using the following command:

Bash

```
pip install -r requirements.txt

```

#### Required Directory Structure

For the pipeline to run successfully, ensure your raw data files are placed exactly as shown below, or let the program make the correct files, but make sure the Kaggle CSV is in the raw folder:

-   `data/raw/jikan/` (Folder for Jikan JSON files)
    
-   `data/raw/anilist/` (Folder for AniList JSON files)
    
-   `data/raw/anime-dataset-2023 2.csv` (The Kaggle Source CSV)
    

----------

### 2. Running the Program

To initiate the full pipeline and launch the terminal, execute the main script from your root directory:

Bash

```
python main.py

```

----------

### 3. The Data Engineering Pipeline

Once started, the program automatically executes a multi-stage ETL (Extract, Transform, Load) process. You will see real-time logs as the system performs the following:

-   **Fetching:** Calls the Jikan (**REST**) and AniList (**GraphQL**) APIs to update local raw data.
    
-   **Cleaning:** Executes PySpark logic to standardize types to `Double`, extract years via **Regex**, and normalize studio names (trimming and case-folding).
    
-   **Integration:** Performs an inner join on all three datasets using `mal_id` as the primary key.
    
-   **Persistence:** Saves the final "Mega Dataset" as a high-performance **Parquet** file for optimized querying.
    

----------

## Anime Big Data Terminal

After the pipeline finishes, an **Interactive Terminal** will open. This allows you to query the processed dataset using Spark SQL logic without needing to write any code.


**1** **System Status**
View basic stats, including total row count and Parquet file size.

**2** **Quick Search**
Search for any anime by title to see its merged metadata.

**3** **Top 10 Rankings**
Displays the highest-rated shows across all combined platforms.

**4** **Industry Trends**
Analyzes production volume and studio performance trends.

**5** **Surprise Me**
Pulls a random highly-rated "hidden gem" from the database.

**6** **Data Dictionary**
Explains the specific fields and data types within the master set.

**7** **Data Health**
**Crucial:** Runs a null-check and zero-check report on all data sources.

**0** **Exit System**
Close the application 

----------

## 🛠️ Project Structure

-   `main.py`: The entry point containing the core pipeline logic and the terminal loop.
    
-   `cleaning_data.py`: The engine containing Spark logic for standardizing disparate sources.
    
-   `fetch_*.py`: Modular scripts dedicated to API interaction and CSV ingestion.
    
-   `checking_health.py`: Quality assurance logic for generating data health reports.
    
-   `queries.py`: A library of pre-defined Spark SQL queries used by the terminal.
    

> [!TIP]
> 
> **Performance Note:** The first run may take a few minutes as Spark processes ~30,000 records and performs complex Regex extractions. Subsequent interactions with the Terminal are near-instant because they read from the optimized `.parquet` file.
--- 
##  Data Dictionary: Mega Anime Dataset

The following table defines the schema for the cleaned and  Consolidated zones of the pipeline. All data is standardized from Jikan, AniList, and Kaggle sources.

**Field Name** |   `Type`  
 Description

**mal_id**       |`Double`  
 Unique ID from MyAnimeList (Primary Key / Join Key)

**title** |`String`
Primary Japanese/Romaji title

**title_english**|`String`
Official English title

**release_year**|`Integer`
Year the anime first aired (extracted via Regex)

**score**|`Double`
Average rating (0.00 to 10.00)

**episodes**|`Double`
Total episodes produced (stored as Double for decimal compatibility)

**duration**|`String`
Length per episode (e.g., '24 min per ep')

**favorites**|`Double`
Total users who favorited the entry

**type**|`String`
Format (TV, Movie, OVA, Special, etc.)

**status**|`String`
Airing status (Finished Airing, Currently Airing, etc.)

**genres**|`Array<String>`
List of genres (Action, Sci-Fi, Award Winning, etc.)

**source**|`String`
Original material (Manga, Light Novel, Original, etc.)

**season**|`String`
Airing season (spring, summer, fall, winter)

**studios**|`Array<String>`
Production companies (Bone, Mappa, etc.)

**themes**|`Array<String>`
Narrative themes (Mecha, Space, Adult Cast, etc.)

**rank**|`Double`
Global popularity/score rank

**rating**|`String`
Age classification (e.g., R - 17+, PG-13)


## Milestone 3 Status and Progress

Status|Task
Description

**Done** |**Data Acquisition**
Successfully fetching 20,000+ records via GraphQL (AniList) and REST (Jikan).

**Done**|**Local Storage**
Implemented a structured raw directory for JSON and Kaggle CSV ingestion.

**Done**|**Verification**
`main.py` successfully displays uniform metadata across disparate sources.

**Done**|**Data Cleaning**
Raw data was fully cleaned and transformed into Parquet.

**Done**|**Dataset Integration**
The 3 data sets where inner join on the mal_id, which acted as my primary key 

**Done**|**Analytical Queries**
The user is presented with a menu to analyze the master dataset.


## 1. Implementation Summary

The primary objective of this project is to aggregate and harmonize anime data from three distinct sources: **Jikan (REST API)**, **AniList (GraphQL)**, and a **Kaggle CSV** dataset.

-   **Jikan:** Representing MyAnimeList data, I scraped 800 pages of JSON data, totaling 20,000 entries.
    
-   **AniList:** Utilized GraphQL to precisely query 20,000 entries, sorted by popularity, to ensure a diverse range of data.
    
-   **Kaggle:** Provided a baseline of 24,905 entries (updated to 2023) to act as a historical anchor.
    

To manage this complexity, I developed a modular architecture:

-   `checking_health.py`: Used to audit datasets for null or "0" values to identify cleaning requirements.
    
-   `cleaning_data.py`: Handles the transformation logic, creates the master set, and saves to **Parquet** for high-speed CPU reads and reduced storage footprint.
    
-   `queries.py`: Contains the business logic for the analytical questions the system answers.
    
-   `main.py`: Serves as the "Master Control Center," providing a unified CLI menu for the user.
    

## 2. Data Flow Description

The data moves through a standard ETL (Extract, Transform, Load) pipeline:

1.  **Extraction:** Raw data is pulled from APIs and local files.
    
2.  **Ingestion:** Data is loaded into Spark DataFrames for high-performance processing.
    
3.  **Transformation:** Jikan serves as our baseline. Unnecessary columns (Japanese titles, background URLs) are dropped. For AniList, I implemented a custom filter for animation studios to ensure column alignment. For Kaggle, headers were remapped to match the master schema.
    
4.  **Integration:** Using the `mal_id` as the primary join key, we combine the sets into a single master record.
    
5.  **Persistence:** The final integrated set is saved in Parquet format.
    

## 3. Transformation Decisions

A major architectural decision was to discontinue the "merge file" strategy for raw JSON data. I realized that merging raw data before cleaning was unnecessary and a significant time sink.

Additionally, I decided to discontinue MongoDB for this stage. After attempting to resolve compatibility issues between Spark and MongoDB, I determined that, for the current requirements, Spark’s native processing and Parquet storage offered a more resilient and less stressful solution for the project’s needs.

## 4. Challenges & Solutions

-   **Database Compatibility:** I spent significant time attempting to bridge Spark and MongoDB, involving multiple version upgrades and downgrades. The solution was to pivot to a **Spark-centric Parquet** workflow, which resolved the bottleneck.
    
-   **Terminal Literacy:** Navigating the terminal was initially difficult; I resolved this through self-guided research and video tutorials to gain the necessary fluency for Big Data tools.
    
-   **Schema Mismatch:** Columns were frequently saved in conflicting data types. I resolved this by standardizing all numerical values as **Doubles** and converting multi-interest fields (genres, studios) into **String Arrays**.
    

## 5. Performance Observations

The most time-consuming phase is the acquisition of raw data. This requires **judicious delays (rate-limiting)** to prevent HTTP 404 or 429 errors from the APIs. While the Spark transformations are currently highly efficient, I will continue to monitor for bottlenecks as we move into more complex M4 analytics.

## 6. Remaining Work

For **M4**, the focus will shift toward:

-   Refining the user interface and potentially developing a **GUI** for a fully functional application.
    
-   Conducting further rigorous testing on joined datasets.
    
-   Codebase optimization, including class separation to improve maintainability and performance.

