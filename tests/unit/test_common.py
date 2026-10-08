import pytest
from pydantic import ValidationError

from src.common.config import Settings
from src.common.retry import RetryExhaustedError, backoff_delay, retry_call
from src.common.sources import ContractError, ContractRegistry, load_sources


def test_backoff_is_exponential_and_capped():
    assert [backoff_delay(n, 0.5, 3) for n in range(1, 6)] == [0.5, 1.0, 2.0, 3, 3]


def test_retry_call_bounded():
    calls, sleeps = [], []
    def boom():
        calls.append(1)
        raise ConnectionError("x")
    with pytest.raises(RetryExhaustedError):
        retry_call(boom, attempts=3, base_delay=0.1, max_delay=1, retry_on=(ConnectionError,), sleep=sleeps.append)
    assert len(calls) == 3 and sleeps == [0.1, 0.2]


def test_retry_call_does_not_retry_unlisted_errors():
    with pytest.raises(ValueError):
        retry_call(lambda: (_ for _ in ()).throw(ValueError("no")), attempts=3, base_delay=0, max_delay=0,
                   retry_on=(ConnectionError,))


@pytest.mark.parametrize("env, value", [
    ("redpanda_brokers", "not a broker"),
    ("ollama_base_url", "ftp://x"),
    ("schema_registry_url", "localhost:8081"),
    ("dlq_topic", "bad topic!"),
    ("confidence_threshold", 1.5),
    ("max_ai_retries", 0),
    ("ollama_model", "qwen; rm -rf"),
])
def test_settings_validation(env, value):
    with pytest.raises(ValidationError):
        Settings(**{env: value})


def test_topics_must_be_distinct():
    with pytest.raises(ValidationError):
        Settings(dlq_topic="cdc.mutations")


def test_secrets_are_not_printed():
    assert "dbz" not in repr(Settings())


MULTI = """
sources:
  shop_eu:
    engine: mysql
    database: inventory
    connector: {class: x, topic_prefix: eu, server_id: 1}
    tables: [{name: products, primary_key: [id], contract: products.json}]
  shop_us:
    engine: mysql
    database: inventory
    connector: {class: x, topic_prefix: us, server_id: 2}
    tables: [{name: products, primary_key: [id], contract: products.json}]
"""


def test_multiple_sources_with_same_table_names_route_separately(tmp_path):
    p = tmp_path / "sources.yaml"
    p.write_text(MULTI)
    reg = ContractRegistry(sources_path=p)
    eu, us = reg.get("inventory", "products", "eu"), reg.get("inventory", "products", "us")
    assert eu.key == "shop_eu.inventory.products" and us.key == "shop_us.inventory.products"
    # database name alone is ambiguous across sources -> no guess
    assert reg.get("inventory", "products", None) is None
    assert reg.get("inventory", "products", "unknown-prefix") is None


def test_duplicate_topic_prefix_rejected(tmp_path):
    p = tmp_path / "sources.yaml"
    p.write_text(MULTI.replace("topic_prefix: us", "topic_prefix: eu"))
    with pytest.raises(ContractError, match="topic_prefix"):
        load_sources(p)


def test_contract_must_match_declared_table(tmp_path):
    p = tmp_path / "sources.yaml"
    p.write_text(MULTI.replace("contract: products.json}]\n  shop_us", "contract: products_on_hand.json}]\n  shop_us"))
    with pytest.raises(ContractError, match="declares"):
        ContractRegistry(sources_path=p)


def test_connector_rendering_per_source(tmp_path, monkeypatch):
    from src.tools.connector import render
    p = tmp_path / "sources.yaml"
    p.write_text(MULTI.replace("class: x", "class: io.debezium.connector.mysql.MySqlConnector"))
    configs = [render(s)["config"] for s in load_sources(p).values()]
    assert [c["topic.prefix"] for c in configs] == ["eu", "us"]
    assert {c["transforms.route.replacement"] for c in configs} == {"cdc.mutations"}
    assert all(c["database.password"] == "${env:MYSQL_PASSWORD}" for c in configs)
    assert configs[0]["database.server.id"] != configs[1]["database.server.id"]
