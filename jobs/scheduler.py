"""
jobs/scheduler.py
─────────────────────────────────────────────────────────────────────────────
Runs Silver + Gold batch jobs on a schedule.
Watches MinIO for new bronze data and triggers the pipeline automatically.

Usage:
    python jobs/scheduler.py                  # default: every 5 minutes
    python jobs/scheduler.py --interval 10    # every 10 minutes
    python jobs/scheduler.py --once           # run once and exit
    python jobs/scheduler.py --silver-only    # skip gold
"""

import sys, os, time, argparse, subprocess
from datetime import datetime
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def run_job(script, label):
    log(f"Starting {label}...")
    start = time.time()
    result = subprocess.run(
        [sys.executable, script],
        capture_output=False,   # stream output live
        text=True,
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    elapsed = round(time.time() - start, 1)
    if result.returncode == 0:
        log(f"{label} complete ({elapsed}s)")
        return True
    else:
        log(f"{label} FAILED after {elapsed}s (exit code {result.returncode})")
        return False


def run_pipeline(silver_only=False):
    log("=" * 50)
    log("Pipeline run starting")
    log("=" * 50)

    base = os.path.dirname(os.path.abspath(__file__))

    # Silver
    silver_ok = run_job(os.path.join(base, "silver_batch.py"), "Silver batch")

    # Gold (only if silver succeeded and not silver_only)
    if silver_ok and not silver_only:
        run_job(os.path.join(base, "gold_batch.py"), "Gold batch")
    elif not silver_ok:
        log("Skipping Gold — Silver failed")

    log("Pipeline run complete")


def check_bronze_has_data():
    """Quick check — returns True if local bronze has any parquet files."""
    bronze = "/tmp/ehr/bronze"
    for table in ["patients", "adt_events", "lab_results", "vitals"]:
        table_path = os.path.join(bronze, table)
        if os.path.exists(table_path):
            for root, dirs, files in os.walk(table_path):
                if any(f.endswith(".parquet") for f in files):
                    return True
    return False


def main():
    parser = argparse.ArgumentParser(description="EHR Pipeline Scheduler")
    parser.add_argument("--interval", type=int, default=5,
                        help="Minutes between pipeline runs (default: 5)")
    parser.add_argument("--once",       action="store_true",
                        help="Run once and exit")
    parser.add_argument("--silver-only", action="store_true",
                        help="Only run silver, skip gold")
    parser.add_argument("--no-wait",    action="store_true",
                        help="Run immediately without waiting for bronze data")
    args = parser.parse_args()

    interval_secs = args.interval * 60

    log(f"EHR Pipeline Scheduler started")
    log(f"  Interval : every {args.interval} minute(s)")
    log(f"  Mode     : {'once' if args.once else 'continuous'}")
    log(f"  Silver   : yes")
    log(f"  Gold     : {'no' if args.silver_only else 'yes'}")

    # Wait for bronze data if needed
    if not args.no_wait:
        log("Checking for bronze data...")
        wait = 0
        while not check_bronze_has_data():
            log(f"No bronze data yet — waiting 30s (total wait: {wait}s)")
            time.sleep(30)
            wait += 30
            if wait > 300:
                log("Waited 5 min with no bronze data — running anyway")
                break
        log("Bronze data found — starting pipeline")

    # Run loop
    run_count = 0
    while True:
        run_count += 1
        log(f"Run #{run_count}")
        try:
            run_pipeline(silver_only=args.silver_only)
        except Exception as e:
            log(f"Pipeline error: {e}")

        if args.once:
            log("--once flag set, exiting")
            break

        log(f"Next run in {args.interval} minute(s). Ctrl-C to stop.")
        try:
            time.sleep(interval_secs)
        except KeyboardInterrupt:
            log("Scheduler stopped by user")
            break


if __name__ == "__main__":
    main()
