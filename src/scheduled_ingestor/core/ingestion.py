from collections.abc import Callable
from datetime import UTC, datetime

from scheduled_ingestor.core.budget import (
    PERSISTENCE_RESERVE_SECONDS,
    SDK_OPERATION_SECONDS,
    InvocationBudget,
)
from scheduled_ingestor.core.http import HttpClient
from scheduled_ingestor.core.provider import BaseProvider
from scheduled_ingestor.core.repository import ObservationRepository


class IngestionService:
    """Fetch, normalize, and persist observations with injected dependencies."""

    def __init__(
        self,
        provider: BaseProvider,
        http: HttpClient,
        repository: ObservationRepository,
        credential_parameter_name: str | None = None,
        ssm_client: object | None = None,
    ) -> None:
        self.provider = provider
        self.http = http
        self.repository = repository
        self.credential_parameter_name = credential_parameter_name
        self.ssm_client = ssm_client
        self._cached_credential: str | None = None

    def _credential(self, budget: InvocationBudget | None = None) -> str | None:
        if not self.credential_parameter_name:
            return None
        if self._cached_credential is None:
            if self.ssm_client is None:
                raise ValueError("Configured credential parameter requires an SSM client")
            budget = budget or InvocationBudget()
            budget.check(PERSISTENCE_RESERVE_SECONDS + SDK_OPERATION_SECONDS)
            with budget.limit(PERSISTENCE_RESERVE_SECONDS, SDK_OPERATION_SECONDS):
                response = self.ssm_client.get_parameter(
                    Name=self.credential_parameter_name,
                    WithDecryption=True,
                )
            self._cached_credential = response["Parameter"]["Value"]
        return self._cached_credential

    def run(self, remaining_time: Callable[[], int] | None = None) -> dict[str, int | str]:
        """Ingest one response, reusing any credential cached by this service.

        Args:
            remaining_time: Optional Lambda remaining-milliseconds callback.

        Returns:
            Status and write counts, including inserts, replacements, and unchanged repeats.
            The built-in repository never skips successful writes, so duplicates is zero.

        Raises:
            ValueError: If a configured credential has no SSM client.
            Exception: If credential access, provider processing, or persistence fails.
        """
        budget = InvocationBudget(remaining_time)
        self.http.set_budget(budget)
        try:
            budget.check(PERSISTENCE_RESERVE_SECONDS)
            observed_at = datetime.now(UTC).replace(microsecond=0)
            credential = self._credential(budget)
            with budget.limit(PERSISTENCE_RESERVE_SECONDS):
                payload = self.provider.fetch(self.http, credential)
            with budget.limit(PERSISTENCE_RESERVE_SECONDS):
                observations = self.provider.normalize(payload, observed_at)
            written = 0
            for observation in observations:
                budget.check(SDK_OPERATION_SECONDS)
                with budget.limit(maximum=SDK_OPERATION_SECONDS):
                    written += self.repository.put(observation)
            budget.check()
            return {"status": "ok", "written": written, "duplicates": len(observations) - written}
        finally:
            self.http.set_budget(None)
