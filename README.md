# Real-Time Cryptocurrency Trade Anomaly Detection Using Apache Kafka

## 1. Project Overview

This project implements a real-time cryptocurrency trade monitoring and anomaly detection pipeline using actual BTC-USD trade data from Coinbase.

The system collects live trades, streams them through Apache Kafka, calculates trade values, identifies unusually large trades, stores processed records in SQLite, and displays monitoring information through a Streamlit dashboard.

## 2. Business Objectives

1. Monitor daily cryptocurrency trading activity, including total trades, total trading value, and average trade value.
2. Identify unusually large trades using a predefined threshold of $5,000.
3. Monitor live trading activity and pipeline performance through a dashboard.

## 3. Architecture

Coinbase Advanced Trade WebSocket
        |
        v
Python Producer
        |
        v
Apache Kafka (crypto-trades)
        |
        v
Python Dashboard Consumer
        |
        v
SQLite Database
        |
        v
Streamlit Dashboard

Coinbase Exchange REST API is also used to retrieve available historical trades after a producer interruption.

## 4. Technologies Used

- Python
- Apache Kafka 4.1.0
- Docker
- Coinbase Advanced Trade WebSocket
- Coinbase Exchange REST API
- SQLite
- Streamlit
- Pandas

## 5. Project Structure

- producer/live_producer.py: Collects live Coinbase trades and publishes them to Kafka.
- producer/recover_gap.py: Historical trade recovery script.
- consumer/dashboard_consumer.py: Consumes Kafka messages and stores trade records.
- dashboard.py: Streamlit monitoring dashboard.
- setup_database.py: Database setup and historical data import.
- results/crypto_trades.db: SQLite database.
- results/live_trades.jsonl: Trade records in JSON Lines format.
- compose.yaml: Kafka Docker configuration.
- requirements.txt: Python dependencies.

## 6. Prerequisites

Install the following:

- Python 3.12 or a compatible Python version.
- Docker Desktop.
- Git (optional).

## 7. Installation

Clone or extract the project and open its root directory.

Create and activate a virtual environment:

    python3 -m venv venv
    source venv/bin/activate

Install dependencies:

    pip install -r requirements.txt

## 8. Start Apache Kafka

Make sure Docker Desktop is running.

From the project root directory, run:

    docker compose up -d

Check the Kafka container:

    docker ps

Kafka is configured on localhost:9092.

## 9. Run the Application

Open three separate Terminal windows.

### Terminal 1: Start the Kafka consumer

Navigate to the project directory and activate the virtual environment:

    cd ~/crypto-anomaly-detection
    source venv/bin/activate

Start the collector:

    python consumer/dashboard_consumer.py

### Terminal 2: Start the producer

Navigate to the project directory and activate the virtual environment:

    cd ~/crypto-anomaly-detection
    source venv/bin/activate

Start the producer:

    python producer/live_producer.py

The producer connects to Coinbase, publishes live BTC-USD trades to Kafka, and checks for available missed trades using the historical REST API.

### Terminal 3: Start the dashboard

Navigate to the project directory and activate the virtual environment:

    cd ~/crypto-anomaly-detection
    source venv/bin/activate

Run:

    streamlit run dashboard.py

Open the local URL displayed by Streamlit, normally:

    http://localhost:8501

## 10. Anomaly Detection Logic

The system calculates:

    trade_value = price * size

A trade is classified as:

- ANOMALY DETECTED: trade value greater than or equal to $5,000.
- NORMAL: trade value below $5,000.

This is a rule-based anomaly detection method, not a machine-learning fraud classifier.

## 11. Reliability and Data Recovery

The producer reads the latest processed BTC-USD trade ID from SQLite.

After a producer interruption, it queries the Coinbase Exchange REST API for available historical trades newer than the stored checkpoint. Retrieved trades are published to Kafka before the producer resumes the live WebSocket stream.

Recovery depends on the historical data available through Coinbase's API and is not guaranteed for unlimited outage periods.

## 12. Idempotency and Duplicate Prevention

The SQLite trades table uses a composite primary key:

    PRIMARY KEY (product, trade_id)

The consumer uses SQLite INSERT OR IGNORE. If a trade is delivered more than once, an existing product and trade ID combination is not inserted again.

This prevents duplicate database records, although Kafka may still deliver duplicate messages.

## 13. Performance Evaluation

The project evaluates the following pipeline measures:

- Throughput: Number of trades processed per unit of time.
- Latency: Time taken to process a trade.
- Scalability: Behaviour under different consumer configurations.
- Reliability: Behaviour during interruptions and recovery.
- Accuracy: Results of controlled anomaly classification tests.

Test result files are available in the results directory. Reported measurements are observations from the performed tests and should not be interpreted as maximum system capacity.

## 14. Reproducibility Notes

Run the consumer before starting the producer so that incoming Kafka messages can be stored.

The dashboard reads the SQLite database and refreshes automatically.

The project uses real Coinbase market data. Availability of live and historical data depends on Coinbase services and network connectivity.

The included database and results files contain records from the development and testing sessions. A fresh reproduction may produce different measurements.

## 15. Author

Individual MSc Applied Data Science and AI project.

Real-Time Cryptocurrency Trade Anomaly Detection Using Apache Kafka.
