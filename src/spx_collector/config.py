from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    tastytrade_client_secret: str | None = Field(
        default=None, alias="TASTYTRADE_CLIENT_SECRET"
    )
    tastytrade_refresh_token: str | None = Field(
        default=None, alias="TASTYTRADE_REFRESH_TOKEN"
    )

    db_url: str = Field(default="sqlite:///spx_options.db", alias="DB_URL")
    underlying_symbol: str = Field(default="SPX", alias="UNDERLYING_SYMBOL")
    option_expiries_per_run: int = Field(default=2, alias="OPTION_EXPIRIES_PER_RUN", ge=1)
    option_strikes_count: int = Field(default=140, alias="OPTION_STRIKES_COUNT", ge=1)
    options_stream_timeout_seconds: int = Field(
        default=20, alias="OPTIONS_STREAM_TIMEOUT_SECONDS", ge=1
    )
    collector_log_level: str = Field(default="INFO", alias="COLLECTOR_LOG_LEVEL")
    collector_debug_events: bool = Field(default=False, alias="COLLECTOR_DEBUG_EVENTS")
    collector_debug_sample_events: int = Field(
        default=3, alias="COLLECTOR_DEBUG_SAMPLE_EVENTS", ge=1
    )
