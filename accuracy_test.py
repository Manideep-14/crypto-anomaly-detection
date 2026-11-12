
THRESHOLD = 5000

test_cases = [
    {"price": 75000, "size": 0.01, "expected": "NORMAL"},
    {"price": 75000, "size": 0.05, "expected": "NORMAL"},
    {"price": 75000, "size": 0.10, "expected": "ANOMALY DETECTED"},
    {"price": 75000, "size": 0.08, "expected": "ANOMALY DETECTED"},
    {"price": 75000, "size": 0.02, "expected": "NORMAL"},
]

correct = 0

for trade in test_cases:
    trade_value = trade["price"] * trade["size"]

    if trade_value >= THRESHOLD:
        predicted = "ANOMALY DETECTED"
    else:
        predicted = "NORMAL"

    if predicted == trade["expected"]:
        correct += 1

    print(
        f"Value: ${trade_value:.2f} | "
        f"Expected: {trade['expected']} | "
        f"Predicted: {predicted}"
    )

accuracy = (correct / len(test_cases)) * 100

print("\n--- Accuracy Test Results ---")
print(f"Total test cases: {len(test_cases)}")
print(f"Correct predictions: {correct}")
print(f"Accuracy: {accuracy:.2f}%")
