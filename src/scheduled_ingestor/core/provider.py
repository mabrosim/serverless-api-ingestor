from abc import ABC, abstractmethod
from datetime import datetime

from scheduled_ingestor.core.http import HttpClient
from scheduled_ingestor.core.models import Observation


class BaseProvider(ABC):
    """Provider adapter interface for fetching and normalizing observations."""

    @abstractmethod
    def fetch(self, client: HttpClient, credential: str | None) -> object:
        """Fetch the provider's raw response payload.

        Args:
            client: HTTP client configured with bounded transport behavior.
            credential: Optional provider credential.

        Returns:
            Raw provider payload for normalization.
        """
        ...

    @abstractmethod
    def normalize(self, payload: object, observed_at: datetime) -> list[Observation]:
        """Convert a raw payload into normalized observations.

        Use API/source timestamps and stable provider, dataset, and subject
        identities where available so repeated source samples retain deterministic
        keys and TTL with unchanged storage settings. Invocation time can change
        on retry; no automatic source-time fallback is supplied.

        Args:
            payload: Raw payload returned by :meth:`fetch`.
            observed_at: UTC invocation time supplied by IngestionService, truncated
                to whole seconds; not a required timestamp for every observation.

        Returns:
            Normalized observations.
        """
        ...

    def readiness_credential(self) -> str | None:
        """Return a nonsecret synthetic credential for offline readiness only.

        This optional, non-abstract hook defaults to None for existing providers.
        Readiness calls it only for a nonempty credential_parameter_name, passing
        its result to fetch with mocked HTTP; otherwise fetch receives None
        without calling the hook.

        Overrides must return obviously synthetic, format-valid content, including
        JSON if fetch expects it. Never return actual secrets or read credentials
        from the environment, SSM, or the network. Production IngestionService
        never calls this hook; fetch must retain strict credential validation.
        Readiness does not normalize observations or prove real authentication.

        Returns:
            Synthetic provider credential, or None by default.
        """
        return None
