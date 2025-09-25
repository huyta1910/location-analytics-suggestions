# Location Analytics Suggestions

## Project Overview

This project helps brands identify potential locations to open new branches by analyzing and visualizing data from various sources. It combines web crawling, data pipelines, and analytics tools to provide actionable insights for expansion decisions.

## Features
- **Web Crawling:** Collects data about restaurants, bars, and entertainment venues from Google Maps using Node.js and Puppeteer.
- **Data Pipeline:** Extracts, transforms, and loads location data into MongoDB and ClickHouse for fast analytics.
- **Analytics & Visualization:** Integrates with tools like Metabase and CloudBeaver for data exploration and hotspot suggestions.
- **Airflow Integration:** Orchestrates ETL workflows for automated data updates.
- **DBT Support:** Enables advanced data modeling and transformation for analytics.

## Use Case
If your brand wants to open a new branch, this project helps you:
- Discover high-potential areas based on competitor density and customer reviews.
- Analyze trends and hotspots in different provinces and cities.
- Make data-driven decisions for expansion.

## Project Structure
```
location-analytics-suggestions/
├── airflow/           # Airflow configs, DAGs, and plugins
├── config/            # Pipeline configuration files
├── crawler/           # Node.js scripts for web crawling
├── datapipeline/      # Python ETL scripts
├── dbt_project/       # DBT models and configs
├── Dockerfile         # Container setup
├── docker-compose.yaml# Multi-service orchestration
├── main.py            # Main Python entry point
```

## Getting Started
1. **Clone the repository:**
   ```
   git clone https://github.com/<your-username>/location-analytics-suggestions.git
   ```
2. **Set up Python environment:**
   ```
   python -m venv .venv
   .\.venv\Scripts\Activate
   pip install -r requirements.txt
   ```
3. **Install Node.js dependencies:**
   ```
   cd crawler
   npm install
   ```
4. **Configure pipeline:**
   Edit `config/pipeline_config.ini` with your database settings.
5. **Run services:**
   ```
   docker-compose up -d
   ```
6. **Start crawling:**
   ```
   node crawler/crawl-restaurant.js
   ```
7. **Run ETL pipeline:**
   ```
   python datapipeline/extract_load.py
   ```

## License
MIT

## Author
huyta
