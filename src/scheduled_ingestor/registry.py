from collections.abc import Callable

from scheduled_ingestor.core.provider import BaseProvider
from scheduled_ingestor.providers.open_meteo_provider import OpenMeteoProvider

PROVIDER_FACTORIES: dict[str, Callable[[str, dict[str, object]], BaseProvider]] = {
    "open_meteo": OpenMeteoProvider,
}


def create_provider(name: str, base_url: str, options: dict[str, object]) -> BaseProvider:
    """Create a registered provider adapter.

    Args:
        name: Registered provider name.
        base_url: Provider HTTPS base URL.
        options: Provider-specific typed options.

    Returns:
        Configured provider adapter.

    Raises:
        ValueError: If no provider is registered under ``name``.
    """
    try:
        factory = PROVIDER_FACTORIES[name]
    except KeyError as error:
        raise ValueError(f"Unknown provider: {name}") from error
    return factory(base_url, options)
