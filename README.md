# ANTINORI Financial Portfolio Reporting System

A full-stack financial portfolio reporting system designed to manage and report investment data for multiple clients. The system processes Excel files, stores data in a PostgreSQL database, and generates detailed portfolio reports at various levels (client, group, portfolio, account, custom).

## Project Overview

The ANTINORI project provides:

- **Data Ingestion**: Processing Excel files for database population
- **Portfolio Reporting**: Generate reports with asset allocations, liquidity, performance metrics, and risk statistics
- **Security**: Encrypted adjusted_value data
- **Frontend Visualization**: User-friendly interface with interactive charts
- **API Endpoints**: RESTful API for data access

## Tech Stack

- **Backend**: Python, Flask, SQLAlchemy, PostgreSQL
- **Frontend**: React, Tailwind CSS
- **Data Processing**: Pandas, openpyxl
- **Encryption**: Fernet (cryptography)
- **Testing**: pytest

## Directory Structure

```
/
├── main.py              # Flask entry point
├── wsgi.py              # WSGI adapter
├── src/                 # Backend source code
│   ├── controllers/     # API endpoint logic
│   ├── models/          # SQLAlchemy models
│   └── utils/           # Utilities (encryption, etc.)
├── frontend/            # React frontend
│   ├── index.html       # HTML entry point
│   └── src/             # Frontend source code
├── data/                # Data directory
│   └── sample_data/     # Sample Excel files
└── tests/               # Test suite
```

## Sample Data Files

The system is designed to process the following Excel files:

1. **data_dump.xlsx**: Contains financial positions data with columns:
   - position, top_level_client, holding_account, holding_account_number, portfolio, cusip, ticker_symbol, asset_class, second_level, third_level, adv_classification, liquid_vs_illiquid, adjusted_value

2. **ownership.xlsx**: Contains ownership hierarchy data with columns:
   - holding_account, holding_account_number, top_level_client, entity_id, portfolio

3. **risk_stats.xlsx**: Contains risk statistics with tabs:
   - Equity tab: position, ticker_symbol, vol, beta
   - Fixed Income tab: position, ticker_symbol, vol, duration
   - Alternatives tab: position, ticker_symbol, vol, beta_to_gold

## Getting Started

### Prerequisites

- Python 3.8+
- PostgreSQL
- Node.js 16+ (for frontend development)

### Installation

1. Clone the repository
2. Install backend dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Set up environment variables:
   - DATABASE_URL: PostgreSQL connection string
   - ENCRYPTION_KEY: Secret key for data encryption

### Running the Application

1. Start the Flask backend:
   ```
   python run.py
   ```
2. The server will be available at http://localhost:5000

### API Endpoints

- `GET /`: API root
- `GET /health`: Health check
- `POST /api/upload/data-dump`: Upload financial positions data
- `POST /api/upload/ownership`: Upload ownership hierarchy data
- `POST /api/upload/risk-stats`: Upload risk statistics data
- `GET /api/ownership-tree`: Get ownership hierarchy as JSON
- `GET /api/portfolio-report`: Generate portfolio report
- `GET /api/charts/allocation`: Get allocation chart data
- `GET /api/charts/liquidity`: Get liquidity chart data
- `GET /api/charts/performance`: Get performance chart data

## License

This project is proprietary and confidential.