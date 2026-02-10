"""
Pytest configuration and fixtures.
"""

import os
import pytest
from pathlib import Path


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Set up test environment variables."""
    os.environ["SOURCE_DB_HOST"] = "localhost"
    os.environ["SOURCE_DB_PORT"] = "3306"
    os.environ["SOURCE_DB_NAME"] = "sakila"
    os.environ["SOURCE_DB_USER"] = "root"
    os.environ["SOURCE_DB_PASSWORD"] = "rootpass"
    
    os.environ["TARGET_DB_HOST"] = "localhost"
    os.environ["TARGET_DB_PORT"] = "5432"
    os.environ["TARGET_DB_NAME"] = "sakila_pg"
    os.environ["TARGET_DB_USER"] = "postgres"
    os.environ["TARGET_DB_PASSWORD"] = "postgrespass"
    
    os.environ["SANDBOX_DB_HOST"] = "localhost"
    os.environ["SANDBOX_DB_PORT"] = "5433"
    os.environ["SANDBOX_DB_NAME"] = "sandbox"
    os.environ["SANDBOX_DB_USER"] = "postgres"
    os.environ["SANDBOX_DB_PASSWORD"] = "postgrespass"
    
    os.environ["GROQ_API_KEY"] = os.getenv("GROQ_API_KEY", "test-key")
    
    yield


@pytest.fixture
def artifacts_dir(tmp_path):
    """Create temporary artifacts directory."""
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    (artifacts / "ddl" / "tables").mkdir(parents=True)
    (artifacts / "procedures").mkdir(parents=True)
    return artifacts


@pytest.fixture
def sample_table_metadata():
    """Sample table metadata for testing."""
    return {
        "name": "actor",
        "schema_name": "public",
        "columns": [
            {"name": "actor_id", "type": "SMALLINT", "nullable": False, "autoincrement": True},
            {"name": "first_name", "type": "VARCHAR(45)", "nullable": False},
            {"name": "last_name", "type": "VARCHAR(45)", "nullable": False},
            {"name": "last_update", "type": "TIMESTAMP", "nullable": False},
        ],
        "primary_key": ["actor_id"],
        "indexes": [{"name": "idx_actor_last_name", "columns": ["last_name"], "unique": False}],
        "foreign_keys": [],
        "row_count": 200,
    }


@pytest.fixture
def sample_mysql_ddl():
    """Sample MySQL DDL for testing."""
    return """
    CREATE TABLE `actor` (
        `actor_id` SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
        `first_name` VARCHAR(45) NOT NULL,
        `last_name` VARCHAR(45) NOT NULL,
        `last_update` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (`actor_id`),
        KEY `idx_actor_last_name` (`last_name`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
