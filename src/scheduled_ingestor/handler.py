import logging

import boto3
from botocore.config import Config

from scheduled_ingestor.core.config import Settings
from scheduled_ingestor.core.http import HttpClient
from scheduled_ingestor.core.ingestion import IngestionService
from scheduled_ingestor.core.repository import ObservationRepository
from scheduled_ingestor.core.transport import bound_sdk_client
from scheduled_ingestor.registry import create_provider

logger = logging.getLogger(__name__)
for _dependency in ("boto3", "botocore", "urllib3"):
    logging.getLogger(_dependency).setLevel(logging.WARNING)
_settings = Settings.from_environment()
_sdk_config = Config(
    connect_timeout=1.0,
    read_timeout=2.0,
    retries={"mode": "standard", "total_max_attempts": 1},
)
_ssm = boto3.client("ssm", config=_sdk_config) if _settings.credential_parameter_name else None
_table = boto3.resource("dynamodb", config=_sdk_config).Table(_settings.table_name)
if _ssm is not None:
    bound_sdk_client(_ssm)
bound_sdk_client(_table.meta.client)
_http = HttpClient()
_provider = create_provider(
    _settings.provider_name, _settings.api_base_url, _settings.provider_options
)
_repository = ObservationRepository(_table, _settings.data_retention_days, _settings.table_schema)
_ingestion = IngestionService(
    _provider,
    _http,
    _repository,
    _settings.credential_parameter_name,
    _ssm,
)


def lambda_handler(event: dict, context: object) -> dict[str, int | str]:
    """Fetch, normalize, and persist one scheduled provider response.

    Args:
        event: Scheduler event payload.
        context: Lambda invocation context.

    Returns:
        Counts of written and duplicate observations.

    Raises:
        Exception: If credential access, provider processing, or persistence fails.
    """
    result = _ingestion.run(context.get_remaining_time_in_millis)
    logger.info(
        "Ingestion completed",
        extra={
            "provider": _settings.provider_name,
            "observation_count": result["written"] + result["duplicates"],
            "written_count": result["written"],
            "duplicate_count": result["duplicates"],
        },
    )
    return result
