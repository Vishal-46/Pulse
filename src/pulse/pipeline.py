"""Run the whole batch pipeline in ONE Spark session, in dependency order:

  extract   Yahoo daily history        -> bronze/bars_1d
  bronze    Kafka topic (available now)-> bronze/bars_1m
  silver    bronze                     -> silver (+ quarantine)
  gold      silver                     -> gold analytics tables
  quality   checks on silver + gold    (fails the run on ERROR)
  report    Spark SQL over gold

    python -m pulse.pipeline                       # everything
    python -m pulse.pipeline --daily-source sample # offline daily history
    python -m pulse.pipeline --steps silver gold quality report
"""

import argparse
import time

from pulse.config import get_settings
from pulse.logging_setup import setup_logging
from pulse.storage import ensure_lake

log = setup_logging("pulse.pipeline")
STEPS = ["extract", "bronze", "silver", "gold", "quality", "report"]


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--steps", nargs="+", choices=STEPS, default=STEPS)
    p.add_argument("--daily-source", choices=["yahoo", "sample"], default="yahoo")
    p.add_argument("--explain", action="store_true", help="print a Gold physical plan")
    a = p.parse_args()

    # Imports inside main: `--help` works instantly without starting a JVM.
    from pulse.analytics import report
    from pulse.ingestion import batch_extract
    from pulse.quality import checks
    from pulse.streaming import bronze_stream
    from pulse.transform import gold, silver
    from pulse.transform.spark import get_spark

    s = get_settings()
    ensure_lake(s)
    spark = get_spark("pulse-pipeline", s)
    actions = {
        "extract": lambda: batch_extract.run(spark, s, source=a.daily_source),
        "bronze": lambda: bronze_stream.run(spark, s, available_now=True),
        "silver": lambda: silver.run(spark, s),
        "gold": lambda: gold.run(spark, s, show_plan=a.explain),
        "quality": lambda: checks.run(spark, s),
        "report": lambda: report.run(spark, s),
    }
    for step in [x for x in STEPS if x in a.steps]:  # always dependency order
        t = time.monotonic()
        log.info("▶ step %s", step)
        actions[step]()
        log.info("✔ step %s done in %.1fs", step, time.monotonic() - t)
    spark.stop()


if __name__ == "__main__":
    main()
