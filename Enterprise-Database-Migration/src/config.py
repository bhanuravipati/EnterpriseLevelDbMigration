"""
Configuration management for AI Database Migration System.
Loads settings from environment variables and .env file.
"""

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseConfig(BaseSettings):
    """Database connection configuration."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="",
        extra="ignore"
    )
    
    # Source Database (MySQL)
    source_db_type: Literal["mysql"] = "mysql"
    source_db_host: str = "localhost"
    source_db_port: int = 3306
    source_db_name: str = "sakila"
    source_db_user: str = "root"
    source_db_password: str = "rootpass"
    
    # Target Database (PostgreSQL)
    target_db_type: Literal["postgresql"] = "postgresql"
    target_db_host: str = "localhost"
    target_db_port: int = 5432
    target_db_name: str = "sakila_pg"
    target_db_user: str = "postgres"
    target_db_password: str = "postgrespass"
    
    # Sandbox Database (PostgreSQL for testing)
    sandbox_db_host: str = "localhost"
    sandbox_db_port: int = 5433
    sandbox_db_name: str = "sandbox"
    sandbox_db_user: str = "postgres"
    sandbox_db_password: str = "postgrespass"
    
    @property
    def source_connection_string(self) -> str:
        """Get SQLAlchemy connection string for source database."""
        return (
            f"mysql+pymysql://{self.source_db_user}:{self.source_db_password}"
            f"@{self.source_db_host}:{self.source_db_port}/{self.source_db_name}"
        )
    
    @property
    def target_connection_string(self) -> str:
        """Get SQLAlchemy connection string for target database."""
        return (
            f"postgresql+psycopg2://{self.target_db_user}:{self.target_db_password}"
            f"@{self.target_db_host}:{self.target_db_port}/{self.target_db_name}"
        )
    
    @property
    def sandbox_connection_string(self) -> str:
        """Get SQLAlchemy connection string for sandbox database."""
        return (
            f"postgresql+psycopg2://{self.sandbox_db_user}:{self.sandbox_db_password}"
            f"@{self.sandbox_db_host}:{self.sandbox_db_port}/{self.sandbox_db_name}"
        )


class LLMConfig(BaseSettings):
    """LLM configuration for Groq API."""
    
    model_config = SettingsConfigDict(env_prefix="", extra="ignore")
    
    groq_api_key: str = Field(default="", description="Groq API Key")
    llm_model_complex: str = "openai/gpt-oss-120b"
    llm_model_fast: str = "llama-3.3-70b-versatile"
    
    # LLM Parameters
    temperature: float = 0.1
    max_tokens: int = 4096
    max_retries: int = 3


class AppConfig(BaseSettings):
    """Application configuration."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )
    
    # Directories
    artifacts_dir: Path = Path("./artifacts")
    reports_dir: Path = Path("./reports")
    
    # Logging
    log_level: str = "INFO"
    
    # Migration settings
    max_retry_attempts: int = 3
    sandbox_enabled: bool = True
    
    def ensure_directories(self) -> None:
        """Create required directories if they don't exist."""
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories for artifacts
        (self.artifacts_dir / "ddl" / "tables").mkdir(parents=True, exist_ok=True)
        (self.artifacts_dir / "ddl" / "views").mkdir(parents=True, exist_ok=True)
        (self.artifacts_dir / "ddl" / "indexes").mkdir(parents=True, exist_ok=True)
        (self.artifacts_dir / "ddl" / "constraints").mkdir(parents=True, exist_ok=True)
        (self.artifacts_dir / "procedures").mkdir(parents=True, exist_ok=True)
        (self.artifacts_dir / "diffs").mkdir(parents=True, exist_ok=True)


class Settings(BaseSettings):
    """Combined settings for the application."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )
    
    db: DatabaseConfig = Field(default_factory=DatabaseConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)
    app: AppConfig = Field(default_factory=AppConfig)
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.app.ensure_directories()


# Global settings instance
_settings: Settings | None = None


def get_settings() -> Settings:
    """Get or create the global settings instance."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reload_settings() -> Settings:
    """Reload settings from environment."""
    global _settings
    _settings = Settings()
    return _settings
