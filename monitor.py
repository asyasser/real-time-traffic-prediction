
import psutil
import time
import csv
from datetime import datetime

OUTPUT_FILE = "resource_usage.csv"

with open(OUTPUT_FILE, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["timestamp", "cpu_percent", "ram_used_gb", "ram_percent"])

print("Monitoring CPU and RAM — Ctrl+C to stop")
print(f"Saving to {OUTPUT_FILE}\n")

try:
    while True:
        cpu = psutil.cpu_percent(interval=1)
        ram = psutil.virtual_memory()
        ram_used_gb = ram.used / (1024**3)
        ram_percent = ram.percent
        ts = datetime.now().strftime("%H:%M:%S")

        print(f"[{ts}] CPU: {cpu:.1f}% | RAM: {ram_used_gb:.2f} GB ({ram_percent:.1f}%)")

        with open(OUTPUT_FILE, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([ts, cpu, round(ram_used_gb, 2), ram_percent])

        time.sleep(2)

except KeyboardInterrupt:
    print(f"\nSaved to {OUTPUT_FILE}")
