import json
import subprocess
import time
import sys

def main():
    # Pass flag and value as separate array items
    cmd = [
        "cargo", "run", "-q", "-p", "collector", "--",
        "--service-name", "python-test-service"
    ]

    process = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    sample_logs = [
        "Unstructured startup log from python app",
        json.dumps({
            "level": "INFO",
            "message": "User logged in",
            "user_id": "usr_9981",
            "module": "auth.py",
            "line": 15
        }),
        json.dumps({
            "level": "ERROR",
            "message": "Failed to connect to redis",
            "retry_count": 3,
            "module": "cache.py",
            "line": 88
        })
    ]

    print("[Python Driver] Streaming logs into collector stdin...")

    try:
        for log in sample_logs:
            # Check if process is still alive before writing
            if process.poll() is not None:
                print(f"[Python Driver] Process exited early with code {process.poll()}")
                break

            process.stdin.write(log + "\n")
            process.stdin.flush()
            time.sleep(0.2)
    except OSError as e:
        print(f"[Python Driver] Pipe write failed (process likely exited): {e}")

    # Close stdin to send EOF to collector
    if process.stdin and not process.stdin.closed:
        process.stdin.close()

    # Capture output
    stdout, stderr = process.communicate()

    print("\n[Python Driver] Collector STDOUT:")
    print(stdout if stdout else "(empty)")

    if stderr:
        print("\n[Python Driver] Collector STDERR:")
        print(stderr)

if __name__ == "__main__":
    main()