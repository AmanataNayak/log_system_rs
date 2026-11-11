import json
import os
import random
import sys
import time

SERVICES = ["auth-service", "payment-service", "data-pipeline", "gateway-proxy"]
LEVELS = ["INFO", "WARN", "ERROR", "DEBUG"]
MODULES = ["db::connection", "auth::jwt", "http::router", "queue::worker"]
FUNCTIONS = ["connect", "validate_token", "handle_request", "process_batch"]
ERRORS = ["Connection reset by peer", "Database pool timeout", "Invalid signature", "Rate limit exceeded", None]

os.makedirs("../data/logs", exist_ok=True)

def generate_log_file(filename, count):
    filepath = os.path.join("../data/logs", filename)
    print(f"Writing {count} logs to {filepath}...", file=sys.stderr)
    start_time = time.time()

    with open(filepath, "a") as f:
        for i in range(1, count + 1):
            level = random.choices(LEVELS, weights=[70, 15, 10, 5])[0]
            selected_error = random.choice(ERRORS) if level in ["WARN", "ERROR"] else None

            log_entry = {
                "timestamp": int(time.time() * 1000),
                "service_name": random.choice(SERVICES),
                "log_level": level,
                "message": f"File event #{i} in {filename}",
                "error": selected_error,
                "module": random.choice(MODULES),
                "function": random.choice(FUNCTIONS),
                "line_no": random.randint(10, 450),
                "metadata": {
                    "request_id": f"file_req_{i:06d}",
                    "tenant_id": f"tenant_{random.randint(1, 20)}",
                    "latency_ms": str(random.randint(2, 150)),
                },
            }
            f.write(json.dumps(log_entry) + "\n")
            if i % 10000 == 0:
                f.flush()
                time.sleep(0.01) # Slight pause to simulate active tailing

    duration = time.time() - start_time
    print(f"Finished writing to {filepath} in {duration:.2f}s", file=sys.stderr)

if __name__ == "__main__":
    # Generate logs across 2 files for the file tailers
    # generate_log_file("service_a.log", 25000)
    generate_log_file("service_b.log", 25000)