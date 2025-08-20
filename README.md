# Dockerized Stock Market Data Pipeline with Airflow

A robust, production-ready data pipeline that automatically fetches current stock prices from Alpha Vantage API and stores them in PostgreSQL using Apache Airflow for orchestration.

## 🎯 Project Overview

This project implements a complete data pipeline that:
- **Fetches Data**: Retrieves current stock prices from Alpha Vantage API (free tier)
- **Processes and Stores**: Parses JSON responses and updates PostgreSQL database
- **Ensures Robustness**: Includes comprehensive error handling and missing data management
- **Scales**: Designed to handle multiple stock symbols with rate limiting
- **Monitors**: Provides detailed logging and monitoring capabilities

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Alpha Vantage │───▶│   Apache Airflow│───▶│   PostgreSQL    │
│      API        │    │   (Orchestrator)│    │   (Database)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🚀 Features

- **Docker Compose**: Single command deployment
- **Apache Airflow**: Professional workflow orchestration
- **Alpha Vantage API**: Reliable stock market data source
- **PostgreSQL**: Robust data storage with upsert capabilities
- **Error Handling**: Comprehensive error management and retry logic
- **Rate Limiting**: Respects API limits with configurable throttling
- **Security**: Environment variable management for sensitive data
- **Scalability**: Designed to handle multiple symbols and increased frequency

## 📋 Prerequisites

- Docker Desktop or Docker Engine + Compose
- Alpha Vantage API key (free at [alphavantage.co](https://www.alphavantage.co/))
- Internet connection for API access

## 🛠️ Quick Start

### 1. Get Alpha Vantage API Key

1. Visit [Alpha Vantage](https://www.alphavantage.co/)
2. Sign up for a free account
3. Copy your API key

### 2. Configure Environment

```bash
# Copy the example environment file
cp env.example .env

# Edit .env with your API key and preferences
nano .env
```

**Required Environment Variables:**
```bash
# Database Configuration
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Airflow Configuration
AIRFLOW_USERNAME=admin
AIRFLOW_PASSWORD=admin
AIRFLOW_EMAIL=admin@example.com
AIRFLOW__WEBSERVER__SECRET_KEY=your_secret_key_here

# Stock Market Data Configuration
STOCK_SYMBOLS=MSFT,AAPL,GOOGL
ALPHA_VANTAGE_API_KEY=your_alpha_vantage_api_key_here
API_THROTTLE_SECONDS=15
```

### 3. Start the Pipeline

```bash
# Build and start all services
docker compose up -d --build

# Check service status
docker compose ps
```

### 4. Access Airflow UI

1. Open your browser and go to `http://localhost:8080`
2. Login with credentials from `.env` (default: `admin/admin`)
3. Find the `stock_market_pipeline` DAG
4. Enable the DAG and trigger a run

## 📊 Database Schema

The pipeline creates and maintains a `stock_prices` table with the following structure:

```sql
CREATE TABLE stock_prices (
    symbol TEXT NOT NULL,
    trading_day DATE NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    adjusted_close NUMERIC,
    volume BIGINT,
    current_price NUMERIC,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, trading_day)
);
```

## 🔧 Configuration Options

### Stock Symbols
Configure which stocks to track in your `.env` file:
```bash
STOCK_SYMBOLS=MSFT,AAPL,GOOGL,TSLA,AMZN
```

### API Throttling
Adjust the delay between API requests to respect rate limits:
```bash
API_THROTTLE_SECONDS=15  # 15 seconds between requests
```

### Schedule
Modify the DAG schedule in `airflow/dags/stock_pipeline_dag.py`:
```python
schedule="@daily"  # Daily at midnight
# Other options: "@hourly", "0 */6 * * *" (every 6 hours), etc.
```

## 🧪 Testing

### Test the API Connection

```bash
# Test locally
python airflow/scripts/test.py

# Test inside the container
docker exec -it byte-airflow-scheduler python /opt/airflow/scripts/test.py
```

### Test the Pipeline Manually

```bash
# Run the pipeline manually inside the container
docker exec -it byte-airflow-scheduler bash -c "
python -c '
import sys; sys.path.append(\"/opt/airflow/scripts\");
from fetch_and_update import fetch_and_update_all;
import os;
fetch_and_update_all([\"MSFT\"], os.environ[\"STOCKS_DB_URI\"])
'
"
```

## 📈 Monitoring and Logs

### View Airflow Logs
```bash
# Scheduler logs
docker compose logs -f airflow-scheduler

# Webserver logs
docker compose logs -f airflow-webserver
```

### Check Database
```bash
# Connect to PostgreSQL
docker exec -it byte-postgres psql -U airflow -d stocks

# Query recent data
SELECT symbol, current_price, last_updated 
FROM stock_prices 
ORDER BY last_updated DESC 
LIMIT 10;
```

## 🔍 Troubleshooting

### Common Issues

1. **API Rate Limiting**
   - Error: "API Notice: Thank you for using Alpha Vantage!"
   - Solution: Increase `API_THROTTLE_SECONDS` in `.env`

2. **Database Connection Issues**
   - Error: "Failed to create database engine"
   - Solution: Ensure PostgreSQL container is running: `docker compose ps`

3. **Missing API Key**
   - Error: "ALPHA_VANTAGE_API_KEY environment variable is required"
   - Solution: Set your API key in `.env` file

4. **Container Startup Issues**
   - Error: "airflow-init service didn't complete successfully"
   - Solution: Check logs: `docker compose logs airflow-init`

### Debug Commands

```bash
# Check all container status
docker compose ps

# View detailed logs
docker compose logs

# Restart specific service
docker compose restart airflow-scheduler

# Rebuild and restart everything
docker compose down -v
docker compose up -d --build
```

## 📁 Project Structure

```
byte/
├── docker-compose.yml          # Main deployment configuration
├── env.example                 # Environment variables template
├── README.md                   # This file
├── sql/
│   └── init_stocks.sql        # Database initialization script
└── airflow/
    ├── requirements.txt        # Python dependencies
    ├── dags/
    │   └── stock_pipeline_dag.py  # Airflow DAG definition
    └── scripts/
        ├── __init__.py
        ├── fetch_and_update.py    # Core data fetching logic
        └── test.py                # Testing script
```

## 🔒 Security Considerations

- **API Keys**: Never commit API keys to version control
- **Database Credentials**: Use strong passwords in production
- **Network Security**: Consider using VPN for production deployments
- **Access Control**: Change default Airflow credentials

## 🚀 Production Deployment

For production use, consider:

1. **Environment Variables**: Use a secure secrets management system
2. **Database**: Use managed PostgreSQL service (AWS RDS, Google Cloud SQL, etc.)
3. **Monitoring**: Add Prometheus/Grafana for metrics
4. **Backup**: Implement automated database backups
5. **Scaling**: Use Kubernetes for container orchestration
6. **Security**: Implement proper authentication and authorization

## 📝 API Limits

- **Alpha Vantage Free Tier**: 5 API calls per minute, 500 per day
- **Current Implementation**: 15-second delay between requests
- **Recommended**: Monitor usage and upgrade to paid tier if needed

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

If you encounter issues:

1. Check the troubleshooting section above
2. Review the logs: `docker compose logs`
3. Test the API connection: `python airflow/scripts/test.py`
4. Open an issue with detailed error information

---

**Happy Data Pipeline Building! 🎉**


