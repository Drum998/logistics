import json
import os
from dataclasses import dataclass


class ConfigError(RuntimeError):
    pass


@dataclass(frozen=True)
class Settings:
    api_base_url: str
    token_url: str
    oauth_basic_user: str
    oauth_basic_password: str
    oauth_username: str
    oauth_password: str
    van_map: dict[str, int]
    overspeed_kph_tolerance: float
    request_delay_ms: int
    max_retries: int


def _getenv_required(name: str) -> str:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        raise ConfigError(f"Missing required environment variable: {name}")
    return v


def _getenv_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    try:
        return float(v)
    except ValueError as e:
        raise ConfigError(f"Invalid float in {name}: {v!r}") from e


def _getenv_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    try:
        return int(v)
    except ValueError as e:
        raise ConfigError(f"Invalid int in {name}: {v!r}") from e


def _getenv_json_dict(name: str) -> dict:
    raw = _getenv_required(name)
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ConfigError(f"{name} must be valid JSON. Got: {raw!r}") from e
    if not isinstance(parsed, dict):
        raise ConfigError(f"{name} must be a JSON object mapping VRN -> vehicleId")
    return parsed


def load_settings(require_secrets: bool = True) -> Settings:
    api_base_url = os.getenv("RAM_API_BASE_URL", "https://api.qaifn.co.uk").rstrip("/")
    token_url = os.getenv("RAM_TOKEN_URL", "https://auth.qaifn.co.uk/oauth/token")

    van_map_raw = _getenv_json_dict("VAN_MAP")
    van_map: dict[str, int] = {}
    for k, v in van_map_raw.items():
        if not isinstance(k, str) or k.strip() == "":
            raise ConfigError("VAN_MAP keys must be non-empty strings (VRNs).")
        try:
            van_map[k.strip()] = int(v)
        except (TypeError, ValueError) as e:
            raise ConfigError(f"VAN_MAP values must be integers. Bad entry {k!r}: {v!r}") from e

    overspeed_tol = _getenv_float("OVERSPEED_KPH_TOLERANCE", 10.0)
    request_delay_ms = _getenv_int("RAM_REQUEST_DELAY_MS", 250)
    max_retries = _getenv_int("RAM_MAX_RETRIES", 5)

    if require_secrets:
        oauth_basic_user = _getenv_required("RAM_OAUTH_BASIC_USER")
        oauth_basic_password = _getenv_required("RAM_OAUTH_BASIC_PASSWORD")
        oauth_username = _getenv_required("RAM_OAUTH_USERNAME")
        oauth_password = _getenv_required("RAM_OAUTH_PASSWORD")
    else:
        oauth_basic_user = os.getenv("RAM_OAUTH_BASIC_USER", "")
        oauth_basic_password = os.getenv("RAM_OAUTH_BASIC_PASSWORD", "")
        oauth_username = os.getenv("RAM_OAUTH_USERNAME", "")
        oauth_password = os.getenv("RAM_OAUTH_PASSWORD", "")

    return Settings(
        api_base_url=api_base_url,
        token_url=token_url,
        oauth_basic_user=oauth_basic_user,
        oauth_basic_password=oauth_basic_password,
        oauth_username=oauth_username,
        oauth_password=oauth_password,
        van_map=van_map,
        overspeed_kph_tolerance=overspeed_tol,
        request_delay_ms=request_delay_ms,
        max_retries=max_retries,
    )

