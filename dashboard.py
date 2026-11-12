
import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

DB_FILE = Path("results/crypto_trades.db")
THRESHOLD = 5000

st.set_page_config(
    page_title="Crypto Anomaly Monitor",
    page_icon="₿",
    layout="wide"
)

st.title("₿ Real-Time Cryptocurrency Trade Anomaly Detection")
st.caption("Coinbase Live Data | Apache Kafka | Python | SQLite | Streamlit")


@st.fragment(run_every="2s")
def live_dashboard():

    if not DB_FILE.exists():
        st.warning("Database not found. Start the Kafka dashboard collector.")
        return

    with sqlite3.connect(DB_FILE) as connection:
        df = pd.read_sql_query(
            "SELECT * FROM trades ORDER BY trade_time DESC",
            connection
        )

    if df.empty:
        st.info("Waiting for live trades...")
        return

    df["trade_datetime"] = pd.to_datetime(
        df["trade_time"], utc=True
    ).dt.tz_convert("Europe/Berlin")

    df["trade_date"] = df["trade_datetime"].dt.strftime("%Y-%m-%d")

    available_dates = sorted(df["trade_date"].unique(), reverse=True)

    st.success("LIVE — Monitoring Coinbase trades through Kafka")

    selected_date = st.selectbox(
        "Select trading date (Hamburg local time)",
        available_dates,
        index=0
    )

    daily = df[df["trade_date"] == selected_date].copy()

    if daily.empty:
        st.warning("No trades available for this date.")
        return

    total_trades = len(daily)
    total_btc = daily["size"].sum()
    total_usd = daily["trade_value"].sum()
    mean_value = daily["trade_value"].mean()
    maximum_trade = daily["trade_value"].max()

    anomalies = int(
        (daily["status"] == "ANOMALY DETECTED").sum()
    )
    normal = total_trades - anomalies
    anomaly_rate = anomalies / total_trades * 100

    st.subheader("Daily Trading Overview")

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Trades Today", f"{total_trades:,}")
    c2.metric("Total BTC Volume", f"{total_btc:.6f} BTC")
    c3.metric("Total Trading Value", f"${total_usd:,.2f}")

    c4, c5, c6 = st.columns(3)
    c4.metric("Mean Trade Value", f"${mean_value:,.2f}")
    c5.metric("Largest Trade", f"${maximum_trade:,.2f}")
    c6.metric("Average BTC Price", f"${daily['price'].mean():,.2f}")

    st.divider()

    st.subheader("Anomaly Detection & Monitoring")

    a1, a2, a3 = st.columns(3)
    a1.metric("Anomalies Detected", f"{anomalies:,}")
    a2.metric("Normal Trades", f"{normal:,}")
    a3.metric("Anomaly Rate", f"{anomaly_rate:.2f}%")

    st.subheader("Average Trade Value per Minute")

    chart_data = (
        daily.set_index("trade_datetime")
        .resample("1min")["trade_value"]
        .mean()
        .dropna()
    )

    st.line_chart(
        chart_data,
        y_label="Average Trade Value (USD)"
    )

    st.divider()

    st.subheader("Real-Time System Performance")

    valid_latency = (
        df.sort_values("processed_at", ascending=False)
        .head(100)["latency_ms"]
        .dropna()
    )

    average_latency = (
        valid_latency.mean() if not valid_latency.empty else 0
    )

    valid_throughput = daily["throughput"].dropna()

    latest_throughput = (
        valid_throughput.iloc[0]
        if not valid_throughput.empty else 0
    )

    p1, p2, p3 = st.columns(3)
    p1.metric(
        "Average Live Processing Latency (Latest 100 Trades)",
        f"{average_latency:.2f} ms"
    )
    p2.metric(
        "Latest Recorded Throughput",
        f"{latest_throughput:.2f}/sec"
    )
    p3.metric("Detection Threshold", "$5,000")

    st.divider()

    st.subheader("Latest Live Trades")

    latest = df.sort_values(
        "trade_datetime", ascending=False
    ).head(30).copy()

    display = latest[
        [
            "trade_datetime", "trade_id", "price", "size",
            "trade_value", "status", "latency_ms"
        ]
    ].copy()

    display["trade_datetime"] = display[
        "trade_datetime"
    ].dt.strftime("%Y-%m-%d %H:%M:%S")

    display = display.rename(columns={
        "trade_datetime": "Time (Hamburg)",
        "trade_id": "Trade ID",
        "price": "BTC Price (USD)",
        "size": "BTC Size",
        "trade_value": "Trade Value (USD)",
        "status": "Detection Status",
        "latency_ms": "Latency (ms)"
    })

    def highlight_status(row):
        if row["Detection Status"] == "ANOMALY DETECTED":
            return [
                "background-color: #5c2020; color: white"
                if column == "Detection Status" else ""
                for column in row.index
            ]

        return [
            "background-color: #164b32; color: white"
            if column == "Detection Status" else ""
            for column in row.index
        ]

    st.dataframe(
        display.style.apply(highlight_status, axis=1),
        use_container_width=True,
        hide_index=True
    )

    st.caption(
        f"Selected date: {selected_date} | "
        f"Detection rule: Trade value ≥ ${THRESHOLD:,.0f} | "
        "Dashboard refreshes every 2 seconds."
    )


live_dashboard()

