
# Pipeline Health Dashboard

A production-ready Streamlit application designed for monitoring pipeline execution health, SLA adherence, and data quality within a Snowflake environment.

## 🏗 Architecture

The application is structured into modular components for maintainability and scalability:

- **`streamlit_app.py`**: The main entry point and controller layer.
- **`services/`**: Handles data loading from Snowflake (`data_loader.py`).
- **`processing/`**: Contains pure Python logic for data transformations (`transformations.py`).
- **`components/`**: Reusable UI and chart components (`charts.py`).
- **`utils/`**: Helper utilities and algorithms (`scoring.py`, `anomaly.py`).

## 🚀 Execution & Deployment

This application is designed to run natively within **Snowflake Streamlit**. It leverages `snowflake.snowpark.context.get_active_session()` to securely connect to the database without hardcoded credentials.

### Database Dependency
The application relies on the following schema objects (as per client specifications):
- `DB_RETAIL_PRD.CONTROL.DIM_PIPELINE_JOB_TIMELINESS`
- `DB_RETAIL_PRD.CONTROL.DIM_PIPELINE_CONTROL_SOURCE`
- `DB_RETAIL_PRD.CONTROL.DIM_PIPELINE_CONTROL_OUTPUT_COMPLETENESS`
- `DB_RETAIL_PRD.CONTROL.DIM_PIPELINE_CONTROL_UNIQUENESS`
- `DB_RETAIL_PRD.CONTROL.DIM_PIPELINE_CONTROL_INTEGRITY`

### Local Development (View Mode)
If run locally without a Snowflake session, the application will default to **View Mode**, displaying the dashboard layout with empty data structures. This allows for logic verification and UI testing without database connectivity.

## 📊 Key Metrics Logic

- **Pipeline Health Score**: A composite metric driven by:
    - Success Rate (50%)
    - SLA Adherence (20%)
    - Data Quality Score (30%)
- **SLA Breach Detection**: Flags any job execution exceeding the threshold (default: 60 minutes).
- **Weekly Aggregation**: Trends are aggregated weekly based on `PIPELINE_START_TIME`.

## 📦 Requirements
See `environment.yml` or `requirements.txt`.
