import requests

url = "https://api.exchange.coinbase.com/products/BTC-USD/trades"

first = requests.get(url, params={"limit": 5}, timeout=15)
first.raise_for_status()

print("First page:")
for trade in first.json():
    print(trade["trade_id"], trade["time"])

cursor = first.headers.get("CB-AFTER")
print("\nCB-AFTER cursor:", cursor)

if cursor:
    second = requests.get(
        url,
        params={"limit": 5, "after": cursor},
        timeout=15
    )
    second.raise_for_status()

    print("\nSecond page:")
    for trade in second.json():
        print(trade["trade_id"], trade["time"])
else:
    print("No CB-AFTER cursor returned.")
