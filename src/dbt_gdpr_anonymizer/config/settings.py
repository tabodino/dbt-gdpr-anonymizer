from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DataGouvConfig(BaseSettings):
    """Configuration for data.gouv.fr API access"""

    model_config = SettingsConfigDict(
        env_prefix="DATAGOUV_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    api_url: str = Field(
        default="https://www.data.gouv.fr/api/1",
        description="Base URL for data.gouv.fr API",
    )

    dataset_id: str = Field(
        default="annuaire-des-services-publics-nationaux",
        description="Dataset ID to download",
    )

    http_timeout: int = Field(default=30, description="HTTP timeout in seconds")
    http_retries: int = Field(default=3, description="Number of retries")


class LoggingConfig(BaseSettings):
    """Logging configuration"""

    model_config = SettingsConfigDict(
        env_prefix="LOG_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    level: str = Field(default="INFO", description="Logging level")
    format: str = Field(
        default=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan> - "
            "<level>{message}</level>"
        ),
        description="Log format for console output",
    )
    file_path: Path = Path("dbt_project/logs/download_data.log")
    rotation: str = "10 MB"
    retention: str = "10 days"
    compression: str = "zip"


class AppSettings(BaseSettings):
    """Main application settings"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    environment: str = Field(
        default="development",
        pattern="^(development|production)$",
        description="Application environment",
    )

    debug: bool = True

    duckdb_path: str = Field(
        default="./dbt_project/gdpr_anonymizer.duckdb",
        alias="DUCKDB_PATH",
        description="Path to DuckDB database file",
    )

    # Sub-configurations
    datagouv: DataGouvConfig = DataGouvConfig()
    logging: LoggingConfig = LoggingConfig()


# Singleton instance
settings = AppSettings()
