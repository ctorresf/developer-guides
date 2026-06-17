# 📊 LATAM Sales Monitoring Dashboard - FinOps Panel

Interactive dashboard built with Streamlit that monitors consolidated sales KPIs across Latin America, including data quality validation and operational metrics.

## Features

- 📊 **KPI Visualization**: Consolidated sales metrics, successful transactions, and regional average ticket
- ⚠️ **Quality Alerts**: Automatic monitoring of Dead-Letter Queue (DLQ) records for data contract validation
- 📋 **Detailed Analysis**: Complete Gold layer view with all consolidated operation data
- 🎯 **Control Panel**: Reactive interface with intelligent alerts for data contract failures

## Requirements

- Python 3.8+
- Dependencies listed in `requirements.txt`

## Installation

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

## Running the Dashboard

To start the dashboard, run the following command from the project root:

```bash
streamlit run src/app.py
```

The dashboard will automatically open in your browser at `http://localhost:8501`

## Project Structure

```
├── README.md                    # This file
├── requirements.txt             # Project dependencies
├── data/
│   ├── gold_sales.csv          # Consolidated sales data (Gold layer)
│   └── dlq_errors.json         # Dead-Letter Queue error records
└── src/
    └── app.py                  # Main Streamlit application
```

## Data

- **gold_sales.csv**: Contains consolidated LATAM sales transaction data
- **dlq_errors.json**: Records rejected entries that do not meet the data contract

## Notes

- The application automatically monitors data quality and displays alerts if there are records in the DLQ
- If all data is valid, it will show a successful data contract validation status
