import json
import random
import sys
import time

SERVICES = ["auth-service", "payment-service", "data-pipeline", "gateway-proxy"]
LEVELS = ["INFO", "WARN", "ERROR", "DEBUG"]
MODULES = ["db::connection", "auth::jwt", "http::router", "queue::worker"]
FUNCTIONS = ["connect", "validate_token", "handle_request", "process_batch"]

ERRORS = [
    "Connection reset by peer",
    "Database pool timeout",
    "Invalid signature",
    "Rate limit exceeded",
    None,
]

def main():
    total_logs = 500000
    print(f"--- Starting 50,00,000 Structured Log Stream ---", file=sys.stderr)
    start_time = time.time()

    for i in range(1, total_logs + 1):
        level = random.choices(LEVELS, weights=[70, 15, 10, 5])[0]
        selected_error = random.choice(ERRORS) if level in ["WARN", "ERROR"] else None

        log_entry = {
            "timestamp": int(time.time() * 1000),
            "service_name": random.choice(SERVICES),
            "log_level": level,
            "message": f"Processed item event #{i} during high-load stream",
            "error": selected_error,
            "module": random.choice(MODULES),
            "function": random.choice(FUNCTIONS),
            "line_no": random.randint(10, 450),
            "metadata": {
                "request_id": f"req_{i:06d}",
                "tenant_id": f"tenant_{random.randint(1, 20)}",
                "latency_ms": str(random.randint(2, 150)),
            },
        }

        # Print JSON payload to stdout for Collector to read
        sys.stdout.write(json.dumps(log_entry) + "\n")

    sys.stdout.flush()
    duration = time.time() - start_time
    print(f"--- Finished streaming 50,00,000 logs in {duration:.2f}s ---", file=sys.stderr)

if __name__ == "__main__":
    main()