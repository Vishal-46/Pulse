"""Central configuration.

Every setting comes from environment variables (prefix PULSE_) or a local .env file.
The same code then runs on the host, inside Docker, or against Azure just by changing env vars,
the "config in the environment" rule from the twelve-factor app.
"""

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="PULSE_", env_file=PROJECT_ROOT / ".env", extra="ignore"
    )

    # --- Kafka / Redpanda ---
    # Host processes use the EXTERNAL listener (localhost:19092); containers use redpanda:9092.
    kafka_bootstrap: str = "localhost:19092"
    topic_bars: str = "market.bars.1m"
    topic_dlq: str = "market.bars.dlq"
    topic_partitions: int = 3

    # --- Object storage (S3 API: RustFS locally) ---
    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = Field(default="pulseadmin")
    s3_secret_key: str = Field(default="pulseadmin-secret")
    lake_bucket: str = "pulse-lake"

    # --- Lake layout ---
    # "s3" -> s3a://bucket/...   "local" -> ./lake/...   "azure" -> abfss://... (Level 2)
    lake_backend: str = "s3"
    local_lake_dir: Path = PROJECT_ROOT / "lake"

    # --- Market data ---
    symbols_file: Path = PROJECT_ROOT / "data" / "reference" / "symbols.csv"
    poll_seconds: int = 60
    market_tz: str = "Asia/Kolkata"

    # --- Spark (CPU, local mode) ---
    spark_master: str = "local[*]"
    spark_driver_memory: str = "2g"
    spark_shuffle_partitions: int = 12  # default 200 is absurd for MBs of data on 12 cores

    @property
    def lake_root(self) -> str:
        if self.lake_backend == "s3":
            return f"s3a://{self.lake_bucket}"
        if self.lake_backend == "local":
            return str(self.local_lake_dir)
        raise ValueError(f"Unsupported lake_backend={self.lake_backend!r}")

    def path(self, *parts: str) -> str:
        """Build a lake path, e.g. settings.path('bronze', 'bars_1m')."""
        return "/".join([self.lake_root.rstrip("/"), *parts])


@lru_cache
def get_settings() -> Settings:
    return Settings()
