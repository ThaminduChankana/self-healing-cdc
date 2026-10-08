"""Centralized, validated configuration.

All runtime configuration comes from environment variables (optionally loaded
from `.env`). Nothing else in the code base reads `os.environ` directly, so the
same code runs on a laptop, in Docker Compose or on a cloud platform by only
changing the environment.
"""

from __future__ import annotations

import re
from enum import Enum
from pathlib import Path
from urllib.parse import urlparse

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

_TOPIC_RE = re.compile(r"^[A-Za-z0-9._-]{1,249}$")
_BROKER_RE = re.compile(r"^[A-Za-z0-9.\-_]+:\d{1,5}$")


class LLMMode(str, Enum):
    OLLAMA = "ollama"
    MOCK = "mock"


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── Source MySQL (capture user; read by Debezium) ──────────────────────
    mysql_host: str = "localhost"
    mysql_port: int = Field(default=3306, ge=1, le=65535)
    mysql_user: str = "debezium"
    mysql_password: SecretStr = SecretStr("dbz")
    mysql_database: str = "inventory"
    # Read-only inspection user (contract generation, drift discovery).
    mysql_reader_user: str = "cdc_reader"
    mysql_reader_password: SecretStr = SecretStr("cdc_reader_pw")
    # Simulated upstream application (seed data + controlled drift only).
    mysql_writer_user: str = "app_writer"
    mysql_writer_password: SecretStr = SecretStr("app_writer_pw")

    # ── SQL sandbox (isolated MySQL; AI SQL only ever runs here) ───────────
    sandbox_db_host: str = "localhost"
    sandbox_db_port: int = Field(default=3307, ge=1, le=65535)
    sandbox_db_user: str = "sandbox_user"
    sandbox_db_password: SecretStr = SecretStr("sandbox_pass")

    # ── Redpanda / Kafka ───────────────────────────────────────────────────
    redpanda_brokers: str = "localhost:19092"
    schema_registry_url: str = "http://localhost:18081"
    debezium_connect_url: str = "http://localhost:8083"

    # ── LLM ────────────────────────────────────────────────────────────────
    llm_mode: LLMMode = LLMMode.OLLAMA
    # 127.0.0.1, not localhost: another container publishing :11434 on IPv6 (::1) would shadow native Ollama.
    ollama_base_url: str = "http://127.0.0.1:11434"
    ollama_model: str = "qwen2.5-coder:7b"
    ollama_timeout_seconds: int = Field(default=240, ge=5, le=1800)
    ollama_num_ctx: int = Field(default=4096, ge=1024, le=32768)

    # ── Topics ─────────────────────────────────────────────────────────────
    mutations_topic: str = "cdc.mutations"
    validated_topic: str = "cdc.validated"
    dlq_topic: str = "cdc.dlq"
    repaired_topic: str = "cdc.repaired"
    audit_topic: str = "cdc.audit"

    # ── Behaviour ──────────────────────────────────────────────────────────
    log_level: str = "INFO"
    ai_repair_enabled: bool = True
    auto_apply_safe_migrations: bool = False
    compatible_type_change_routing: str = "dlq"  # dlq | validated
    confidence_threshold: float = Field(default=0.90, ge=0.0, le=1.0)
    max_ai_retries: int = Field(default=3, ge=1, le=10)
    max_consumer_retries: int = Field(default=5, ge=1, le=50)
    retry_base_delay_seconds: float = Field(default=0.5, gt=0, le=60)
    retry_max_delay_seconds: float = Field(default=30.0, gt=0, le=600)
    llm_unavailable_backoff_max_seconds: float = Field(default=60.0, gt=0, le=3600)
    sandbox_timeout_seconds: int = Field(default=5, ge=1, le=60)
    poll_timeout_seconds: float = Field(default=1.0, gt=0, le=30)

    # ── Observability ──────────────────────────────────────────────────────
    metrics_port: int = Field(default=8001, ge=0, le=65535)  # 0 disables

    # ── Paths ──────────────────────────────────────────────────────────────
    sources_config_path: Path = PROJECT_ROOT / "config" / "sources.yaml"
    canonical_schema_dir: Path = PROJECT_ROOT / "config" / "canonical_schema"
    migration_policy_path: Path = PROJECT_ROOT / "config" / "policies" / "migration_policy.yaml"
    type_compatibility_path: Path = PROJECT_ROOT / "config" / "policies" / "type_compatibility.yaml"
    rename_hints_path: Path = PROJECT_ROOT / "config" / "mappings" / "rename_hints.yaml"
    state_dir: Path = PROJECT_ROOT / "state"

    # ── Validators ─────────────────────────────────────────────────────────
    @field_validator("redpanda_brokers")
    @classmethod
    def _validate_brokers(cls, v: str) -> str:
        brokers = [b.strip() for b in v.split(",") if b.strip()]
        if not brokers:
            raise ValueError("REDPANDA_BROKERS must not be empty")
        for b in brokers:
            if not _BROKER_RE.match(b):
                raise ValueError(f"Invalid broker address '{b}' (expected host:port)")
        return ",".join(brokers)

    @field_validator("schema_registry_url", "ollama_base_url", "debezium_connect_url")
    @classmethod
    def _validate_url(cls, v: str) -> str:
        parsed = urlparse(v)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError(f"Invalid URL '{v}' (expected http(s)://host:port)")
        return v.rstrip("/")

    @field_validator("ollama_model")
    @classmethod
    def _validate_model(cls, v: str) -> str:
        if not re.match(r"^[A-Za-z0-9._\-/]+(:[A-Za-z0-9._\-]+)?$", v):
            raise ValueError(f"Invalid model name '{v}'")
        return v

    @field_validator(
        "mutations_topic", "validated_topic", "dlq_topic", "repaired_topic", "audit_topic"
    )
    @classmethod
    def _validate_topic(cls, v: str) -> str:
        if not _TOPIC_RE.match(v):
            raise ValueError(f"Invalid topic name '{v}'")
        return v

    @field_validator("log_level")
    @classmethod
    def _validate_log_level(cls, v: str) -> str:
        v = v.upper()
        if v not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError(f"Invalid LOG_LEVEL '{v}'")
        return v

    @field_validator("compatible_type_change_routing")
    @classmethod
    def _validate_routing(cls, v: str) -> str:
        v = v.lower()
        if v not in {"dlq", "validated"}:
            raise ValueError("COMPATIBLE_TYPE_CHANGE_ROUTING must be 'dlq' or 'validated'")
        return v

    @model_validator(mode="after")
    def _distinct_topics(self) -> "Settings":
        topics = [
            self.mutations_topic, self.validated_topic, self.dlq_topic,
            self.repaired_topic, self.audit_topic,
        ]
        if len(set(topics)) != len(topics):
            raise ValueError("Pipeline topic names must be distinct")
        if self.retry_max_delay_seconds < self.retry_base_delay_seconds:
            raise ValueError("RETRY_MAX_DELAY_SECONDS must be >= RETRY_BASE_DELAY_SECONDS")
        return self

    @property
    def state_db_path(self) -> Path:
        return self.state_dir / "state.db"

    @property
    def warehouse_db_path(self) -> Path:
        return self.state_dir / "warehouse.db"


_settings: Settings | None = None


def get_settings() -> Settings:
    """Return the cached settings instance."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reset_settings() -> None:
    """Drop the cached settings (tests change the environment between cases)."""
    global _settings
    _settings = None
