"""Market bar producer: Yahoo (or a recorded file) -> Redpanda topic `market.bars.1m`.

Modes
  live    poll Yahoo every PULSE_POLL_SECONDS and publish newly CLOSED 1-minute bars
  replay  publish bars recorded in data/replay/*.jsonl, sped up (demo any time of day)
  record  save today's 1-minute bars to data/replay/ for later replay

Run:  python -m pulse.ingestion.producer live
"""

from __future__ import annotations

import argparse
import csv
import json
import signal
import time
from datetime import UTC, datetime
from pathlib import Path

from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from pulse.config import PROJECT_ROOT, Settings, get_settings
from pulse.logging_setup import setup_logging
from pulse.schemas import Bar
from pulse.sources import yahoo

log = setup_logging("pulse.producer")
REPLAY_DIR = PROJECT_ROOT / "data" / "replay"


def load_symbols(s: Settings) -> list[str]:
    with open(s.symbols_file) as f:
        return [row["symbol"] for row in csv.DictReader(f)]


# ---------------------------------------------------------------- topic admin
def ensure_topics(s: Settings) -> None:
    """Create topics if missing. Partition count is a design decision:
    it caps consumer parallelism and cannot be reduced later without recreating the topic."""
    admin = AdminClient({"bootstrap.servers": s.kafka_bootstrap})
    existing = admin.list_topics(timeout=10).topics
    wanted = [
        NewTopic(s.topic_bars, num_partitions=s.topic_partitions, replication_factor=1,
                 config={"retention.ms": str(7 * 24 * 3600 * 1000)}),  # 7 days of replayable history
        NewTopic(s.topic_dlq, num_partitions=1, replication_factor=1),
    ]
    todo = [t for t in wanted if t.topic not in existing]
    if not todo:  # idempotent: running setup twice must be a no-op, not a crash
        log.info("topics already exist: %s", ", ".join(t.topic for t in wanted))
        return
    for topic, fut in admin.create_topics(todo).items():
        fut.result()  # raises if creation failed
        log.info("created topic %s", topic)


# ---------------------------------------------------------------- producer
class BarPublisher:
    def __init__(self, s: Settings):
        self.s = s
        self.sent = 0
        self.failed = 0
        self.producer = Producer({
            "bootstrap.servers": s.kafka_bootstrap,
            "client.id": "pulse-bar-producer",
            # Durability: the broker acknowledges only after the write is fully replicated.
            "acks": "all",
            # Idempotence: the broker de-dups the producer's internal RETRIES
            # (sequence numbers per partition), so a network blip cannot create duplicates.
            "enable.idempotence": True,
            # Throughput: wait up to 50 ms to batch messages, then compress the batch.
            "linger.ms": 50,
            "compression.type": "zstd",
            "delivery.timeout.ms": 120_000,  # give up on a message after 2 min of retrying
        })

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        """Called asynchronously from poll()/flush() once the broker answers."""
        if err is not None:
            self.failed += 1
            log.error("delivery failed key=%s: %s", msg.key(), err)
        else:
            self.sent += 1
            log.debug("delivered %s -> %s[%d]@%d", msg.key(), msg.topic(), msg.partition(), msg.offset())

    def publish(self, bar: Bar) -> None:
        # KEY = symbol: Kafka hashes the key to choose a partition, so every bar of one symbol
        # lands in the SAME partition, in order. Ordering is only guaranteed within a partition.
        self.producer.produce(
            self.s.topic_bars,
            key=bar.symbol.encode(),
            value=json.dumps(bar.to_message()).encode(),
            headers={"schema_version": str(bar.schema_version)},
            on_delivery=self._on_delivery,
        )
        self.producer.poll(0)  # serve delivery callbacks without blocking

    def publish_reject(self, reject: dict) -> None:
        """Dead-letter queue: invalid records are kept (for debugging), not dropped silently."""
        self.producer.produce(self.s.topic_dlq, value=json.dumps(reject, default=str).encode())

    def flush(self, timeout: float = 30) -> int:
        remaining = self.producer.flush(timeout)
        if remaining:
            log.error("%d messages still undelivered after flush", remaining)
        return remaining


# ---------------------------------------------------------------- modes
_running = True


def _stop(signum, _frame):
    global _running
    log.info("signal %s received, finishing current cycle then exiting", signum)
    _running = False


def run_live(s: Settings) -> None:
    symbols = load_symbols(s)
    pub = BarPublisher(s)
    # High-water mark per symbol: the newest bar timestamp already published.
    # It lives in memory, so after a restart we re-send today's bars. That is
    # AT-LEAST-ONCE delivery, and Silver's dedupe on event_id makes it harmless.
    last_sent: dict[str, datetime] = {}
    log.info("live mode: %d symbols, polling every %ss", len(symbols), s.poll_seconds)
    while _running:
        started = time.monotonic()
        try:
            res = yahoo.fetch_intraday(symbols)
            new = [b for b in res.bars if b.ts > last_sent.get(b.symbol, datetime.min.replace(tzinfo=UTC))]
            for bar in new:
                pub.publish(bar)
                last_sent[bar.symbol] = max(bar.ts, last_sent.get(bar.symbol, bar.ts))
            for r in res.rejects:
                pub.publish_reject(r)
            pub.flush()
            log.info("cycle: fetched=%d new=%d rejects=%d | total sent=%d failed=%d",
                     len(res.bars), len(new), len(res.rejects), pub.sent, pub.failed)
        except Exception:
            # Unlike v1, we log the full traceback and keep NO half-finished state around.
            log.exception("cycle failed; will retry next poll")
        _sleep_until(started + s.poll_seconds)
    pub.flush()


def run_replay(s: Settings, speed: float) -> None:
    """Publish recorded bars in timestamp order. speed=60 -> one recorded minute per second."""
    files = sorted(REPLAY_DIR.glob("*.jsonl"))
    if not files:
        raise SystemExit(f"no replay files in {REPLAY_DIR}; run `record` during market hours first")
    bars = [Bar.model_validate_json(line) for f in files for line in f.read_text().splitlines() if line]
    bars.sort(key=lambda b: b.ts)
    pub = BarPublisher(s)
    log.info("replay mode: %d bars from %d file(s) at %sx", len(bars), len(files), speed)
    prev_ts = None
    for bar in bars:
        if not _running:
            break
        if prev_ts is not None and bar.ts > prev_ts and speed > 0:
            time.sleep((bar.ts - prev_ts).total_seconds() / speed)
        pub.publish(bar.model_copy(update={"source": "replay", "produced_at": datetime.now(UTC)}))
        prev_ts = bar.ts
    pub.flush()
    log.info("replay done: sent=%d failed=%d", pub.sent, pub.failed)


def run_record(s: Settings) -> Path:
    res = yahoo.fetch_intraday(load_symbols(s))
    REPLAY_DIR.mkdir(parents=True, exist_ok=True)
    day = res.bars[0].ts.date().isoformat() if res.bars else "empty"
    out = REPLAY_DIR / f"bars_1m_{day}.jsonl"
    out.write_text("\n".join(b.model_dump_json() for b in res.bars) + "\n")
    log.info("recorded %d bars (%d rejects) -> %s", len(res.bars), len(res.rejects), out)
    return out


def _sleep_until(deadline: float) -> None:
    while _running and time.monotonic() < deadline:
        time.sleep(min(1.0, deadline - time.monotonic()))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("mode", choices=["live", "replay", "record"])
    p.add_argument("--speed", type=float, default=60.0, help="replay speed-up factor (0 = no delay)")
    args = p.parse_args()
    s = get_settings()
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)  # what `docker stop` sends
    if args.mode == "record":
        run_record(s)
        return
    ensure_topics(s)
    run_live(s) if args.mode == "live" else run_replay(s, args.speed)


if __name__ == "__main__":
    main()
