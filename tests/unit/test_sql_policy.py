import pytest

from src.ai_repair.sql_policy import MigrationPolicy, tokenize, SqlPolicyError
from src.common.models import RiskLevel


@pytest.fixture(scope="module")
def policy():
    return MigrationPolicy()


@pytest.mark.parametrize("sql", [
    "ALTER TABLE products_on_hand MODIFY COLUMN quantity BIGINT NOT NULL",
    "ALTER TABLE `products_on_hand` MODIFY quantity BIGINT NOT NULL;",
])
def test_widening_allowed(policy, on_hand_contract, sql):
    r = policy.validate(sql, on_hand_contract)
    assert r.allowed, r.violations
    assert r.risk == RiskLevel.LOW
    assert r.rendered == ["ALTER TABLE `products_on_hand` MODIFY COLUMN `quantity` BIGINT NOT NULL"]


def test_add_nullable_column_allowed(policy, products_contract):
    r = policy.validate("ALTER TABLE products ADD COLUMN supplier_code VARCHAR(64) NULL DEFAULT 'n/a'", products_contract)
    assert r.allowed, r.violations
    assert r.rendered[0].endswith("VARCHAR(64) NULL DEFAULT 'n/a'")


def test_varchar_widening_allowed(policy, products_contract):
    assert policy.validate("ALTER TABLE products MODIFY name VARCHAR(512) NOT NULL", products_contract).allowed


def test_none_or_blank_is_allowed(policy, products_contract):
    assert policy.validate(None, products_contract).allowed
    assert policy.validate("   ", products_contract).allowed


@pytest.mark.parametrize("sql, reason", [
    ("DROP TABLE products", "forbidden keyword"),
    ("DROP DATABASE inventory", "forbidden keyword"),
    ("TRUNCATE TABLE products", "forbidden keyword"),
    ("DELETE FROM products WHERE 1=1", "forbidden keyword"),
    ("ALTER TABLE products DROP COLUMN weight", "forbidden keyword"),
    ("UPDATE products SET weight = 0", "forbidden keyword"),
    ("GRANT ALL ON *.* TO 'x'@'%'", "forbidden keyword"),
    ("CREATE USER evil IDENTIFIED BY 'x'", "forbidden keyword"),
    ("RENAME TABLE products TO p2", "forbidden keyword"),
    ("ALTER TABLE products ADD COLUMN c INT NULL; DROP TABLE products", "forbidden keyword"),
    ("ALTER TABLE products ADD COLUMN c INT /*! DROP TABLE x */", "comments"),
    ("ALTER TABLE products ADD COLUMN c INT -- sneaky", "comments"),
    ("ALTER TABLE products ADD COLUMN c INT # x", "comments"),
    ("ALTER TABLE mysql.products ADD COLUMN c INT NULL", "schema-qualified"),
    ("ALTER TABLE products ADD COLUMN c INT NOT NULL", "nullable or have a DEFAULT"),
    ("ALTER TABLE products_on_hand ADD COLUMN c INT NULL", "targets table"),
    ("ALTER TABLE products MODIFY COLUMN weight INT NULL", "not a widening"),
    ("ALTER TABLE products MODIFY COLUMN name VARCHAR(10) NOT NULL", "not a widening"),
    ("ALTER TABLE products MODIFY COLUMN description VARCHAR(600) NOT NULL", "NOT NULL"),
    ("ALTER TABLE products CHANGE COLUMN weight wt FLOAT NULL", "rename"),
    ("CREATE INDEX i ON products (name)", "CREATE INDEX is disabled"),
    ("SELECT * FROM products", "not allowed"),
    ("ALTER TABLE products ADD COLUMN c INT NULL DEFAULT 'a\\'b'", "backslash"),
    ('ALTER TABLE products ADD COLUMN c INT NULL DEFAULT "x"', "double-quoted"),
    ("ALTER TABLE products ADD COLUMN c GEOMETRY NULL", "not allowed"),
    ("ALTER TABLE products ADD COLUMN c INT NULL DEFAULT (SELECT 1)", "not an allowed literal"),
    ("ALTER TABLE products ADD COLUMN c INT NULL, ALGORITHM=INPLACE", "not allowed"),
])
def test_dangerous_or_invalid_sql_rejected(policy, products_contract, sql, reason):
    r = policy.validate(sql, products_contract)
    assert not r.allowed
    assert r.risk != RiskLevel.LOW
    assert (r.risk in (RiskLevel.HIGH, RiskLevel.CRITICAL)) == r.destructive
    assert any(reason.lower() in v.lower() for v in r.violations), r.violations


@pytest.mark.parametrize("sql", [
    "ALTER TABLE products ADD COLUMN c INT NOT NULL",
    "CREATE INDEX i ON products (name)",
    "ALTER TABLE products ADD COLUMN c GEOMETRY NULL",
    "ALTER TABLE products ADD COLUMN weight FLOAT NULL",
])
def test_non_conforming_but_harmless_sql_is_retryable(policy, products_contract, sql):
    r = policy.validate(sql, products_contract)
    assert not r.allowed and not r.destructive and r.risk == RiskLevel.MEDIUM


def test_forbidden_word_inside_string_literal_is_data(policy, products_contract):
    r = policy.validate("ALTER TABLE products ADD COLUMN note VARCHAR(20) NULL COMMENT 'do not drop'",
                        products_contract)
    assert r.allowed, r.violations


def test_tokenizer_rejects_unknown_characters():
    with pytest.raises(SqlPolicyError):
        tokenize("ALTER TABLE t ADD c INT NULL @x")


def test_qualified_name_matching_contract_database_is_canonicalised(policy, on_hand_contract):
    r = policy.validate("ALTER TABLE inventory.products_on_hand MODIFY COLUMN quantity BIGINT NOT NULL", on_hand_contract)
    assert r.allowed, r.violations
    assert r.rendered == ["ALTER TABLE `products_on_hand` MODIFY COLUMN `quantity` BIGINT NOT NULL"]


def test_foreign_schema_qualifier_is_destructive(policy, on_hand_contract):
    r = policy.validate("ALTER TABLE mysql.products_on_hand MODIFY COLUMN quantity BIGINT NOT NULL", on_hand_contract)
    assert not r.allowed and r.destructive


def test_unnecessary_add_of_existing_column_is_not_destructive(policy, on_hand_contract):
    # what qwen2.5-coder:7b actually proposed for a rename in the first live run
    r = policy.validate("ALTER TABLE inventory.products_on_hand ADD COLUMN quantity INT NULL;", on_hand_contract)
    assert not r.allowed
    assert not r.destructive
    assert any("already exists" in v for v in r.violations)


@pytest.mark.parametrize("sql", ["DROP TABLE products", "ALTER TABLE products MODIFY COLUMN weight INT NULL",
                                 "ALTER TABLE products ADD COLUMN c INT /*! x */"])
def test_dangerous_sql_is_marked_destructive(policy, products_contract, sql):
    assert policy.validate(sql, products_contract).destructive


def test_no_op_modify_is_retryable_not_verified(policy, on_hand_contract):
    # also observed live from qwen2.5-coder:7b for a pure rename
    r = policy.validate("ALTER TABLE inventory.products_on_hand MODIFY COLUMN quantity INT NOT NULL", on_hand_contract)
    assert not r.allowed and not r.destructive
    assert any("no-op" in v for v in r.violations)
