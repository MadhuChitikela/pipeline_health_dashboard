
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

### Local Development (Simulation Mode)
The application includes a **SQLite Simulation Layer** for robust local testing without Snowflake access. 
- It automatically detects when `snowflake.snowpark` session is missing.
- It connects to a local SQLite database (`local_simulation.db`) that mimics the exact production schema.
- This allows for full end-to-end verification of UI logic, data transformations, and KPI calculations.

To initialize local data:
```bash
python setup_local_db.py
streamlit run streamlit_app.py
```

## 📊 Key Metrics Logic

- **Pipeline Health Score**: A composite metric driven by:
    - Success Rate (50%)
    - SLA Adherence (20%)
    - Data Quality Score (30%)
- **SLA Breach Detection**: Flags any job execution exceeding the threshold (default: 60 minutes).
- **Weekly Aggregation**: Trends are aggregated weekly based on `PIPELINE_START_TIME`.

## 📦 Requirements
See `environment.yml` or `requirements.txt`.
