# Airbnb NYC 2019 Data Pipeline

## Project Overview

This project is designed to process and analyze Airbnb listings data for New York City. It includes scripts for data cleaning, transformation, and analysis, as well as an Airflow DAG for orchestrating these tasks.

## Project Structure

```
.
├── data_cleaning.py        # script for cleaning the data (Task 5)
├── data_transformation.py  # script for transforming the data and loading it into the database (Task 5)
├── data_analysis.py        # script for analyzing the data (Task 5)
├── batch.py                # standalone script for batch processing (Task 2)
├── airflow_dag.py          # Airflow DAG for orchestrating tasks in the data pipeline (Task 5)
├── .env                    # Environment variables for database connection
├── requirements.txt        # Python dependencies
├── GC_Data_exercise.ipynb  # Jupyter notebook with data cleaning, preprocessing, warehousing, analysis and visualization (Tasks 1, 3, 4)
├── report.pdf              # Project report
└── README.md               # Project documentation
```

## Instructions

### Prerequisites

- Python 3.8 or higher
- PostgreSQL 13 or higher
- Apache Airflow 2.0 or higher

### Setup

1. **Install dependencies:**

    ```sh
    pip install -r requirements.txt
    ```

2. **Set up environment variables:**

    Create a `.env` file in the project root with the following content:

    ```dotenv
    DB_USER=<username>
    DB_PASS=<your_password>
    DB_HOST=<localhost>
    DB_PORT=<port>
    DB_NAME=ABNB_NYC_2019
    ```
   
3. **Create a PostgreSQL database:**

    ```sh
    brew install postgresql # if not already installed
    psql -U <username> -c "CREATE DATABASE ABNB_NYC_2019;"
    psql -U <username> -h localhost -p <port> -d ABNB_NYC_2019 # connect to the database
    \dt # list tables, table should be here after running the scripts
    ```

### Running the Scripts

**Batch Processing:**

    ```sh
    python3 batch.py
    ```
**Data Cleaning:**

    ```sh
    python3 data_cleaning.py
    ```

**Data Transformation:**

    ```sh
    python3 data_transformation.py
    ```

**Data Analysis:**

    ```sh
    python3 data_analysis.py
    ```

### Running the Airflow DAG
Alternative to running the data cleaning, data transformation, and data analysis scripts individually and manually.

1. **Enable the DAG:**

    Copy the `airflow_dag.py` file to the `dags` directory in your Airflow installation.
    Update the filepath variables as necessary in the DAG file to ensure that the scripts are executed correctly.

    ```sh
    cp airflow_dag.py <airflow_home>/dags
    ```

1. **Start Airflow:**

    ```sh
    airflow db init
    airflow scheduler
    airflow webserver --port 8080
    ```

2. **OPTIONAL: Create a User:**

    ```sh
    airflow users create \
        --username admin \
        --firstname <your_firstname> \
        --lastname <your_lastname> \
        --role Admin \
        --email <your_email>
    ```

2. **Access the Airflow UI:**

    Open your web browser and go to `http://localhost:8080`.

3. **Trigger the DAG:**

    - Navigate to the DAGs tab.
    - Find the `data_pipeline` DAG.
    - Click the toggle to enable the DAG.
    - Click the play button to trigger the DAG. 