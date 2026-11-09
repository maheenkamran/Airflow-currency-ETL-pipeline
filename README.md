# Airflow Currency Exchange ETL Pipeline

An automated daily data pipeline that collects currency exchange rates, cleans them up, and stores them in a database, built with **Apache Airflow (Astro CLI)**, **MongoDB**, and **Docker**.

## Introduction

Exchange rates change every day, and checking them by hand is slow and error-prone. This project solves that with an **ETL pipeline** (Extract, Transform, Load) that runs on its own every day.

## Pipeline Flow

```
Frankfurter API → Extract → Transform → Load → MongoDB → Mongo Express (view data)
```

| Task | What it does |
|------|--------------|
| **Extract** | Calls the Frankfurter API and gets raw exchange rates |
| **Transform** | Converts the raw data into the required document format |
| **Load** | Saves the documents into MongoDB (no duplicates on re-runs) |

Tasks pass data to each other using **named XComs** (`raw_rates` and `transformed_rates`).

##  Tech Stack

| Technology | Purpose |
|------------|---------|
| **Apache Airflow** | Schedules and runs the pipeline tasks in order |
| **Astro CLI** | Sets up and runs Airflow locally |
| **Python** | Language used to write the tasks |
| **Frankfurter API** | Free public source of currency exchange rates |
| **MongoDB** | Database where the final data is stored |
| **Mongo Express** | Web interface for viewing the MongoDB data |
| **Docker** | Runs every service in its own container |

## Author

Maheen Kamran Zeb