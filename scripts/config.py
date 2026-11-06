#!/usr/bin/env python3
import argparse
import inspect
import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock, patch

import yaml

from scheduled_ingestor.core.config import Settings
from scheduled_ingestor.core.http import HttpClient
from scheduled_ingestor.core.provider import BaseProvider
from scheduled_ingestor.core.storage import TableSchema
from scheduled_ingestor.registry import PROVIDER_FACTORIES, create_provider

REQUIRED_MODULES = {
    "__init__.py",
    "handler.py",
    "registry.py",
    "core/__init__.py",
    "core/config.py",
    "core/http.py",
    "core/ingestion.py",
    "core/models.py",
    "core/provider.py",
    "core/repository.py",
    "core/storage.py",
    "providers/__init__.py",
    "providers/open_meteo_provider.py",
}
OBSOLETE_MODULES = {
    "config.py",
    "http.py",
    "models.py",
    "repository.py",
    "storage.py",
    "providers/base.py",
    "providers/registry.py",
    "providers/open_meteo.py",
}
ALLOWED_RETENTION = {1, 3, 5, 7, 14, 30, 60, 90}
INVALID_BASE_URL_MESSAGE = "provider.base_url must use HTTPS"
URL_PORT_PATTERN = (
    r"(?:[1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])"
)
BASE_URL_PATTERN = (
    r"^https://(?:localhost|(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?[.])+"
    r"[A-Za-z]{2,63}|(?:25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])"
    r"(?:[.](?:25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])){3})"
    rf"(?::{URL_PORT_PATTERN})?(?:/[A-Za-z0-9._~%!$&'()*+,;=:@/-]*)?"
    r"(?:[?][A-Za-z0-9._~%!$&'()*+,;=:@/?-]*)?$"
)
SSM_PARAMETER_PATTERN = (
    r"^$|/(?=.{1,2047}$)(?!(?:[Aa][Ww][Ss]|[Ss][Ss][Mm])(?:/|$))"
    r"(?:[A-Za-z0-9_.-]+/){0,14}[A-Za-z0-9_.-]+$"
)
SSM_PARAMETER_MAX_LENGTH = 2048
STACK_NAME_MAX_LENGTH = 55
STACK_NAME_PATTERN = r"^[A-Za-z][A-Za-z0-9-]{0,54}$"
SCHEDULE_EXPRESSION_MAX_LENGTH = 256
ENVIRONMENT_PATTERN = r"^[a-z][a-z0-9-]{1,15}$"
PROVIDER_NAME_PATTERN = r"^[a-z][a-z0-9_-]{1,31}$"
ALARM_TOPIC_ARN_PATTERN = r"^arn:[A-Za-z0-9-]+:sns:[A-Za-z0-9-]+:\d{12}:[A-Za-z0-9_-]+$"
CRON_STEP_PATTERN = r"[1-9][0-9]*"
CRON_MINUTE_VALUE_PATTERN = r"(?:[0-9]|[1-5][0-9])"
CRON_HOUR_VALUE_PATTERN = r"(?:[0-9]|1[0-9]|2[0-3])"
CRON_DAY_OF_MONTH_VALUE_PATTERN = r"(?:[1-9]|[12][0-9]|3[01])"
CRON_MONTH_VALUE_PATTERN = r"(?:[1-9]|1[0-2]|JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)"
CRON_DAY_OF_WEEK_VALUE_PATTERN = r"(?:[1-7]|SUN|MON|TUE|WED|THU|FRI|SAT)"
CRON_YEAR_VALUE_PATTERN = r"(?:19(?:7[0-9]|[89][0-9])|2[01][0-9]{2})"

CRON_MONTH_VALUES = {
    name: index
    for index, name in enumerate(
        (
            "JAN",
            "FEB",
            "MAR",
            "APR",
            "MAY",
            "JUN",
            "JUL",
            "AUG",
            "SEP",
            "OCT",
            "NOV",
            "DEC",
        ),
        start=1,
    )
}
CRON_DAY_OF_WEEK_VALUES = {
    name: index
    for index, name in enumerate(("SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"), start=1)
}


def _cron_regular_term(value_pattern: str) -> str:
    return rf"(?:\*|{value_pattern}(?:-{value_pattern})?)(?:/{CRON_STEP_PATTERN})?"


def _cron_list(term_pattern: str) -> str:
    return rf"{term_pattern}(?:,{term_pattern})*"


CRON_MINUTE_FIELD_PATTERN = _cron_list(_cron_regular_term(CRON_MINUTE_VALUE_PATTERN))
CRON_HOUR_FIELD_PATTERN = _cron_list(_cron_regular_term(CRON_HOUR_VALUE_PATTERN))
CRON_DAY_OF_MONTH_FIELD_PATTERN = _cron_list(
    rf"(?:{_cron_regular_term(CRON_DAY_OF_MONTH_VALUE_PATTERN)}|L|"
    rf"{CRON_DAY_OF_MONTH_VALUE_PATTERN}W)"
)
CRON_MONTH_FIELD_PATTERN = _cron_list(_cron_regular_term(CRON_MONTH_VALUE_PATTERN))
CRON_DAY_OF_WEEK_FIELD_PATTERN = _cron_list(
    rf"(?:{_cron_regular_term(CRON_DAY_OF_WEEK_VALUE_PATTERN)}|"
    rf"{CRON_DAY_OF_WEEK_VALUE_PATTERN}L|{CRON_DAY_OF_WEEK_VALUE_PATTERN}#[1-5])"
)
CRON_YEAR_FIELD_PATTERN = _cron_list(_cron_regular_term(CRON_YEAR_VALUE_PATTERN))
SCHEDULE_RATE_PATTERN_TEXT = (
    r"rate\((?:1 (?:minute|hour|day)|(?:[2-9]|[1-9][0-9]+) "
    r"(?:minutes|hours|days))\)"
)
SCHEDULE_CRON_PATTERN_TEXT = (
    rf"cron\({CRON_MINUTE_FIELD_PATTERN}\s+{CRON_HOUR_FIELD_PATTERN}\s+"
    rf"(?:{CRON_DAY_OF_MONTH_FIELD_PATTERN}\s+{CRON_MONTH_FIELD_PATTERN}\s+\?\s+"
    rf"{CRON_YEAR_FIELD_PATTERN}|\?\s+{CRON_MONTH_FIELD_PATTERN}\s+"
    rf"{CRON_DAY_OF_WEEK_FIELD_PATTERN}\s+{CRON_YEAR_FIELD_PATTERN})\)"
)
SCHEDULE_RATE_PATTERN = re.compile(rf"^{SCHEDULE_RATE_PATTERN_TEXT}$", re.ASCII)
SCHEDULE_CRON_PATTERN = re.compile(rf"^{SCHEDULE_CRON_PATTERN_TEXT}$", re.ASCII)
SCHEDULE_EXPRESSION_PATTERN = re.compile(
    rf"^(?:{SCHEDULE_RATE_PATTERN_TEXT}|{SCHEDULE_CRON_PATTERN_TEXT})$", re.ASCII
)
LAMBDA_ENVIRONMENT_SIZE_LIMIT = 4096
DYNAMODB_ATTRIBUTE_TYPES = {"S", "N", "B"}
STORAGE_KEYS = {
    "table_name",
    "billing_mode",
    "provisioned_throughput",
    "partition_key",
    "sort_key",
    "ttl",
    "point_in_time_recovery",
}
APPLICATION_KEYS = {"stack_name", "environment"}
PROVIDER_REQUIRED_KEYS = {"name", "base_url"}
PROVIDER_OPTIONAL_KEYS = {"credential_parameter_name", "options"}
SCHEDULE_KEYS = {
    "expression",
    "enabled",
    "flexible_time_window",
    "maximum_retry_attempts",
    "maximum_event_age_seconds",
}
RUNTIME_KEYS = {"memory_mb", "timeout_seconds", "architecture"}
LOGGING_KEYS = {"level", "retention_days"}
ALARMS_REQUIRED_KEYS = {"enabled"}
ALARMS_OPTIONAL_KEYS = {"notification_topic_arn"}
KEY_KEYS = {"name", "type"}
THROUGHPUT_KEYS = {"read_capacity_units", "write_capacity_units"}
TTL_KEYS = {"attribute_name", "retention_days"}
TABLE_NAME_PATTERN = re.compile(r"[A-Za-z0-9_.-]{3,255}")
ATTRIBUTE_NAME_PATTERN = re.compile(r"[A-Za-z0-9_.-]{1,255}")


def validate_section(
    value: object,
    label: str,
    required_keys: set[str],
    optional_keys: frozenset[str] = frozenset(),
) -> dict:
    """Validate a configuration mapping's key set.

    Args:
        value: Candidate configuration section.
        label: Configuration path used in validation errors.
        required_keys: Keys that must be present.
        optional_keys: Keys that may be present.

    Returns:
        The validated mapping.

    Raises:
        ValueError: If the section is not a mapping or has invalid keys.
    """
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a mapping")
    if any(not isinstance(key, str) for key in value):
        raise ValueError(f"{label} keys must be strings")
    keys = set(value)
    unknown = keys - required_keys - optional_keys
    missing = required_keys - keys
    if unknown or missing:
        raise ValueError(f"Invalid {label}; missing={sorted(missing)}, unknown={sorted(unknown)}")
    return value


def validate_key(value: object, label: str) -> None:
    """Validate a configured DynamoDB key definition.

    Args:
        value: Candidate key definition.
        label: Configuration path used in validation errors.

    Returns:
        None.

    Raises:
        ValueError: If the key definition or attribute type is invalid.
    """
    if not isinstance(value, dict) or set(value) != KEY_KEYS:
        raise ValueError(f"storage.{label} must contain only name and type")
    if not isinstance(value["name"], str) or not ATTRIBUTE_NAME_PATTERN.fullmatch(value["name"]):
        raise ValueError(f"storage.{label}.name is invalid")
    if not isinstance(value["type"], str) or value["type"] not in DYNAMODB_ATTRIBUTE_TYPES:
        raise ValueError(f"storage.{label}.type must be one of {sorted(DYNAMODB_ATTRIBUTE_TYPES)}")


def validate_table_name(table_name: object) -> None:
    """Validate a DynamoDB table name.

    Args:
        table_name: Candidate table name.

    Returns:
        None.

    Raises:
        ValueError: If the table name is outside the supported pattern.
    """
    if not isinstance(table_name, str) or not TABLE_NAME_PATTERN.fullmatch(table_name):
        raise ValueError("storage.table_name is invalid")


def validate_billing_mode(billing_mode: object) -> None:
    """Validate the supported DynamoDB billing mode.

    Args:
        billing_mode: Candidate billing mode.

    Returns:
        None.

    Raises:
        ValueError: If the billing mode is not provisioned.
    """
    if billing_mode != "PROVISIONED":
        raise ValueError("storage.billing_mode must be PROVISIONED")


def _validate_int_range(value: object, label: str, minimum: int, maximum: int) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or not minimum <= value <= maximum:
        raise ValueError(f"{label} must be an integer from {minimum} through {maximum}")


def validate_throughput(throughput: object) -> None:
    """Validate provisioned DynamoDB capacity settings.

    Args:
        throughput: Candidate read and write capacity mapping.

    Returns:
        None.

    Raises:
        ValueError: If capacity settings are missing or outside supported limits.
    """
    if not isinstance(throughput, dict) or set(throughput) != THROUGHPUT_KEYS:
        raise ValueError(
            "storage.provisioned_throughput must contain read and write capacity units"
        )
    for capacity_key, capacity_units in throughput.items():
        if (
            not isinstance(capacity_units, int)
            or isinstance(capacity_units, bool)
            or not 1 <= capacity_units <= 40000
        ):
            raise ValueError(
                f"storage.provisioned_throughput.{capacity_key} must be 1 through 40000"
            )


def validate_ttl(ttl: object) -> None:
    """Validate DynamoDB TTL configuration.

    Args:
        ttl: Candidate TTL configuration.

    Returns:
        None.

    Raises:
        ValueError: If the TTL attribute or retention period is invalid.
    """
    if not isinstance(ttl, dict) or set(ttl) != TTL_KEYS:
        raise ValueError("storage.ttl must contain only attribute_name and retention_days")
    if not isinstance(ttl["attribute_name"], str) or not ATTRIBUTE_NAME_PATTERN.fullmatch(
        ttl["attribute_name"]
    ):
        raise ValueError("storage.ttl.attribute_name is invalid")
    if (
        not isinstance(ttl["retention_days"], int)
        or isinstance(ttl["retention_days"], bool)
        or not 1 <= ttl["retention_days"] <= 3650
    ):
        raise ValueError("storage.ttl.retention_days must be 1 through 3650")


def validate_storage(storage: object) -> None:
    """Validate the complete DynamoDB storage configuration.

    Args:
        storage: Candidate storage configuration.

    Returns:
        None.

    Raises:
        ValueError: If storage settings or cross-field constraints are invalid.
    """
    if not isinstance(storage, dict) or set(storage) != STORAGE_KEYS:
        raise ValueError(f"storage must contain only {sorted(STORAGE_KEYS)}")
    validate_table_name(storage["table_name"])
    validate_billing_mode(storage["billing_mode"])
    validate_throughput(storage["provisioned_throughput"])
    validate_key(storage["partition_key"], "partition_key")
    validate_key(storage["sort_key"], "sort_key")
    if storage["partition_key"]["name"] == storage["sort_key"]["name"]:
        raise ValueError("storage partition and sort key names must differ")
    validate_ttl(storage["ttl"])
    if not isinstance(storage["point_in_time_recovery"], bool):
        raise ValueError("storage.point_in_time_recovery must be boolean")
    ttl_name = storage["ttl"]["attribute_name"]
    if ttl_name in {storage["partition_key"]["name"], storage["sort_key"]["name"]}:
        raise ValueError("storage.ttl.attribute_name must differ from key names")


def runtime_storage_schema(storage: dict, options: dict) -> dict:
    """Return the storage fields required by the Lambda runtime.

    Args:
        storage: Validated storage configuration.
        options: Provider options with selected measurement declarations.

    Returns:
        Runtime storage schema mapping.
    """
    fields = [
        storage["partition_key"],
        storage["sort_key"],
        {"name": storage["ttl"]["attribute_name"], "type": "N"},
    ]
    measurements = options.get("measurements")
    if not isinstance(measurements, list) or not measurements:
        raise ValueError("provider.options.measurements must be a non-empty list")
    for measurement in measurements:
        if not isinstance(measurement, dict) or not {"name", "type"} <= set(measurement):
            raise ValueError("Selected measurements must declare name and type")
        fields.append({"name": measurement["name"], "type": measurement["type"]})
    schema = {
        "partition_key": storage["partition_key"],
        "sort_key": storage["sort_key"],
        "ttl": storage["ttl"],
        "fields": fields,
    }
    return TableSchema.from_mapping(schema).to_mapping()


def validate_provider_options(options: object) -> None:
    """Validate provider options as a typed mapping.

    Args:
        options: Candidate provider options mapping.

    Returns:
        None.

    Raises:
        ValueError: If keys are not strings.
    """
    if not isinstance(options, dict) or any(not isinstance(key, str) for key in options):
        raise ValueError("provider.options must be a mapping with string keys")


def validate_provider_base_url(base_url: object) -> None:
    """Validate the conservative HTTPS URL contract used by the SAM template.

    Args:
        base_url: Candidate provider base URL.

    Returns:
        None.

    Raises:
        ValueError: If the URL is not an accepted HTTPS URL.
    """
    if not isinstance(base_url, str):
        raise ValueError(INVALID_BASE_URL_MESSAGE)
    if re.fullmatch(BASE_URL_PATTERN, base_url) is None:
        raise ValueError(INVALID_BASE_URL_MESSAGE)


def validate_stack_name(value: object, label: str = "application.stack_name") -> None:
    """Validate a stack name and its derived resource names.

    Args:
        value: Candidate CloudFormation stack name.
        label: Configuration path or environment variable name used in errors.

    Returns:
        None.

    Raises:
        ValueError: If the stack name exceeds the derived resource limit.
    """
    if not isinstance(value, str) or re.fullmatch(STACK_NAME_PATTERN, value) is None:
        raise ValueError(f"{label} is invalid")


def validate_application(application: object) -> None:
    """Validate CloudFormation stack and environment names.

    Args:
        application: Candidate application configuration.

    Returns:
        None.

    Raises:
        ValueError: If an application value is outside template constraints.
    """
    application = validate_section(application, "application", APPLICATION_KEYS)
    validate_stack_name(application["stack_name"])
    if (
        not isinstance(application["environment"], str)
        or re.fullmatch(ENVIRONMENT_PATTERN, application["environment"]) is None
    ):
        raise ValueError("application.environment is invalid")


def validate_provider_name(name: object) -> None:
    """Validate the provider name against the SAM parameter constraint.

    Args:
        name: Candidate provider name.

    Returns:
        None.

    Raises:
        ValueError: If the provider name is invalid.
    """
    if not isinstance(name, str) or re.fullmatch(PROVIDER_NAME_PATTERN, name) is None:
        raise ValueError("provider.name is invalid")


def validate_logging(logging: object) -> None:
    """Validate logging values supported by the template.

    Args:
        logging: Candidate logging configuration.

    Returns:
        None.

    Raises:
        ValueError: If a logging value is invalid.
    """
    logging = validate_section(logging, "logging", LOGGING_KEYS)
    if not isinstance(logging["level"], str) or logging["level"] not in {
        "DEBUG",
        "INFO",
        "WARNING",
        "ERROR",
        "CRITICAL",
    }:
        raise ValueError("logging.level must be one of DEBUG, INFO, WARNING, ERROR, CRITICAL")
    retention_days = logging["retention_days"]
    if (
        not isinstance(retention_days, int)
        or isinstance(retention_days, bool)
        or retention_days not in ALLOWED_RETENTION
    ):
        raise ValueError(f"logging.retention_days must be one of {sorted(ALLOWED_RETENTION)}")


def validate_alarms(alarms: object) -> None:
    """Validate optional CloudWatch alarm configuration.

    Args:
        alarms: Candidate alarm configuration.

    Returns:
        None.

    Raises:
        ValueError: If an alarm value is invalid.
    """
    alarms = validate_section(
        alarms,
        "alarms",
        ALARMS_REQUIRED_KEYS,
        frozenset(ALARMS_OPTIONAL_KEYS),
    )
    if not isinstance(alarms["enabled"], bool):
        raise ValueError("alarms.enabled must be boolean")
    topic_arn = alarms.get("notification_topic_arn")
    if topic_arn is not None and (
        not isinstance(topic_arn, str)
        or re.fullmatch(ALARM_TOPIC_ARN_PATTERN, topic_arn, flags=re.ASCII) is None
    ):
        raise ValueError("alarms.notification_topic_arn must be null or a valid SNS topic ARN")


def _cron_range_value(value: str, aliases: dict[str, int]) -> int:
    return int(value) if value.isdecimal() else aliases[value]


def _validate_cron_field_ranges(field: str, label: str, aliases: dict[str, int]) -> None:
    for term in field.split(","):
        range_term = term.split("/", 1)[0]
        if "-" not in range_term:
            continue
        start, end = range_term.split("-", 1)
        if _cron_range_value(start, aliases) > _cron_range_value(end, aliases):
            raise ValueError(f"schedule.expression contains descending range in {label}")


def validate_cron_ranges(expression: str) -> None:
    """Reject cron ranges whose start exceeds their end value.

    Args:
        expression: Validated six-field EventBridge cron expression.

    Returns:
        None.

    Raises:
        ValueError: If any cron range is descending.
    """
    fields = expression[5:-1].split()
    if fields[4].count("#") > 1:
        raise ValueError("schedule.expression contains multiple # expressions in day of week")
    for field, label, aliases in (
        (fields[0], "minute", {}),
        (fields[1], "hour", {}),
        (fields[2], "day of month", {}),
        (fields[3], "month", CRON_MONTH_VALUES),
        (fields[4], "day of week", CRON_DAY_OF_WEEK_VALUES),
        (fields[5], "year", {}),
    ):
        _validate_cron_field_ranges(field, label, aliases)


def validate_schedule(schedule: object) -> None:
    """Validate schedule scalars supported by the fixed SAM schedule.

    Args:
        schedule: Validated schedule configuration.

    Returns:
        None.

    Raises:
        ValueError: If a schedule value is invalid.
    """
    schedule = validate_section(schedule, "schedule", SCHEDULE_KEYS)
    expression = schedule["expression"]
    if (
        not isinstance(expression, str)
        or len(expression) > SCHEDULE_EXPRESSION_MAX_LENGTH
        or (
            SCHEDULE_RATE_PATTERN.fullmatch(expression) is None
            and SCHEDULE_CRON_PATTERN.fullmatch(expression) is None
        )
    ):
        raise ValueError(
            "schedule.expression must be a valid rate or cron expression no more than "
            f"{SCHEDULE_EXPRESSION_MAX_LENGTH} characters"
        )
    if expression.startswith("cron("):
        validate_cron_ranges(expression)
    if not isinstance(schedule["enabled"], bool):
        raise ValueError("schedule.enabled must be boolean")
    if schedule["flexible_time_window"] is not False:
        raise ValueError("schedule.flexible_time_window must be false")
    _validate_int_range(schedule["maximum_retry_attempts"], "schedule.maximum_retry_attempts", 0, 5)
    _validate_int_range(
        schedule["maximum_event_age_seconds"],
        "schedule.maximum_event_age_seconds",
        60,
        86400,
    )


def validate_runtime(runtime: object) -> None:
    """Validate runtime values supported by the ARM64-only template.

    Args:
        runtime: Validated runtime configuration.

    Returns:
        None.

    Raises:
        ValueError: If a runtime value is invalid.
    """
    runtime = validate_section(runtime, "runtime", RUNTIME_KEYS)
    if runtime["architecture"] != "arm64":
        raise ValueError("runtime.architecture must be arm64")
    _validate_int_range(runtime["memory_mb"], "runtime.memory_mb", 128, 10240)
    _validate_int_range(runtime["timeout_seconds"], "runtime.timeout_seconds", 1, 60)


def lambda_environment_values(config: dict) -> dict[str, str]:
    """Return the Lambda environment values generated from validated config.

    Args:
        config: Validated application configuration mapping.

    Returns:
        Environment variable names and their serialized values.
    """
    provider = config["provider"]
    storage = config["storage"]
    return {
        "TABLE_NAME": storage["table_name"],
        "PROVIDER_NAME": provider["name"],
        "API_BASE_URL": provider["base_url"],
        "CREDENTIAL_PARAMETER_NAME": provider.get("credential_parameter_name") or "",
        "PROVIDER_OPTIONS_JSON": json.dumps(provider.get("options", {}), separators=(",", ":")),
        "DATA_RETENTION_DAYS": str(storage["ttl"]["retention_days"]),
        "STORAGE_SCHEMA_JSON": json.dumps(
            runtime_storage_schema(storage, provider["options"]), separators=(",", ":")
        ),
    }


def validate_lambda_environment_size(config: dict) -> None:
    """Reject config whose generated Lambda environment exceeds 4 KiB.

    Args:
        config: Validated application configuration mapping.

    Returns:
        None.

    Raises:
        ValueError: If generated environment variables exceed the Lambda limit.
    """
    environment = lambda_environment_values(config)
    total_bytes = sum(len(f"{name}={value}".encode()) for name, value in environment.items())
    if total_bytes > LAMBDA_ENVIRONMENT_SIZE_LIMIT:
        raise ValueError(
            "Lambda environment variables require "
            f"{total_bytes} bytes; maximum is {LAMBDA_ENVIRONMENT_SIZE_LIMIT} bytes"
        )


def load(path: str | Path) -> dict:
    """Load and validate an application configuration file.

    Args:
        path: YAML configuration file path.

    Returns:
        Validated configuration mapping.

    Raises:
        ValueError: If YAML or any configuration value is invalid.
    """
    try:
        data = yaml.safe_load(Path(path).read_text())
    except yaml.YAMLError as error:
        raise ValueError(f"Invalid YAML in {path}") from error
    required = {
        "application",
        "provider",
        "schedule",
        "runtime",
        "logging",
        "storage",
        "alarms",
    }
    data = validate_section(data, "top-level config", required)
    application = validate_section(data["application"], "application", APPLICATION_KEYS)
    stack = os.environ.get("STACK_NAME", application["stack_name"])
    stack_label = "STACK_NAME" if "STACK_NAME" in os.environ else "application.stack_name"
    validate_stack_name(stack, stack_label)
    data["application"]["stack_name"] = stack
    validate_application(application)
    if isinstance(data["storage"], dict):
        if data["storage"].get("table_name") is None:
            data["storage"]["table_name"] = f"{stack}-observations"
        else:
            print(
                "Warning: explicit storage.table_name disables stack-derived table isolation",
                file=sys.stderr,
            )
    provider = validate_section(
        data["provider"],
        "provider",
        PROVIDER_REQUIRED_KEYS,
        frozenset(PROVIDER_OPTIONAL_KEYS),
    )
    schedule = validate_section(data["schedule"], "schedule", SCHEDULE_KEYS)
    runtime = validate_section(data["runtime"], "runtime", RUNTIME_KEYS)
    validate_schedule(schedule)
    validate_runtime(runtime)
    logging = data["logging"]
    validate_logging(logging)
    validate_storage(data["storage"])
    validate_alarms(data["alarms"])
    validate_provider_name(provider["name"])
    if provider["name"] not in PROVIDER_FACTORIES:
        raise ValueError("Unknown provider")
    validate_provider_base_url(provider["base_url"])
    validate_provider_options(provider.get("options", {}))
    parameter = provider.get("credential_parameter_name")
    if parameter is not None and not isinstance(parameter, str):
        raise ValueError("Invalid SSM parameter name")
    if parameter and re.fullmatch(SSM_PARAMETER_PATTERN, parameter) is None:
        raise ValueError("Invalid SSM parameter name")
    validate_lambda_environment_size(data)
    create_provider(provider["name"], provider["base_url"], provider.get("options", {}))
    return data


class TemplateLoader(yaml.SafeLoader):
    """Read SAM intrinsics without evaluating cloud resources."""


def _intrinsic(loader: TemplateLoader, tag: str, node: yaml.Node) -> dict:
    if isinstance(node, yaml.ScalarNode):
        value = loader.construct_scalar(node)
    elif isinstance(node, yaml.SequenceNode):
        value = loader.construct_sequence(node)
    else:
        value = loader.construct_mapping(node)
    return {tag: value}


TemplateLoader.add_multi_constructor("!", _intrinsic)


def validate_rendered_environment(config: dict) -> None:
    """Verify template wiring agrees with the runtime configuration locally."""
    template = yaml.load(
        Path(__file__).parents[1].joinpath("template.yaml").read_text(), Loader=TemplateLoader
    )
    values = _parameter_values(config)
    values["ObservationsTable"] = config["storage"]["table_name"]
    function = template["Resources"]["FetcherFunction"]["Properties"]
    variables = {
        **template["Globals"]["Function"].get("Environment", {}).get("Variables", {}),
        **function["Environment"]["Variables"],
    }
    rendered = {
        name: values[value["Ref"]] if isinstance(value, dict) else value
        for name, value in variables.items()
    }
    if rendered != lambda_environment_values(config):
        raise ValueError("Rendered SAM environment differs from runtime configuration")
    with patch.dict(os.environ, rendered):
        settings = Settings.from_environment()
    if settings.table_schema.to_mapping() != runtime_storage_schema(
        config["storage"], config["provider"]["options"]
    ):
        raise ValueError("Runtime schema differs from deployment selections")


def readiness(path: str | Path) -> dict:
    """Check local readiness without API, SSM, or AWS access.

    Args:
        path: Authoritative config or sanitized test fixture.

    Returns:
        Completed configuration.

    Raises:
        ValueError: If configuration, request, or template wiring is unfinished.
    """
    try:
        config = load(path)
    except ValueError:
        print("FAIL config: fill required values; correct named path in error")
        print("SKIP provider/request/schema/runtime/SAM env: config unfinished")
        raise
    print("PASS required config / effective stack-table naming / schedule / runtime / log limits")
    provider = create_provider(
        config["provider"]["name"], config["provider"]["base_url"], config["provider"]["options"]
    )
    if not isinstance(provider, BaseProvider):
        raise ValueError("Registered provider must inherit BaseProvider")
    client = Mock(spec=HttpClient)
    credential = (
        provider.readiness_credential()
        if config["provider"].get("credential_parameter_name")
        else None
    )
    provider.fetch(client, credential)
    if client.get_json.call_count != 1:
        raise ValueError("Provider readiness must construct one bounded HTTPS request")
    request = client.get_json.call_args
    try:
        bound_request = inspect.signature(HttpClient.get_json).bind(
            client, *request.args, **request.kwargs
        )
    except TypeError as error:
        raise ValueError(
            "Provider readiness must construct a valid HttpClient.get_json call"
        ) from error
    validate_provider_base_url(bound_request.arguments["url"])
    print("PASS provider ABC / options / constructed HTTPS request / selected mappings")
    validate_rendered_environment(config)
    print("PASS required typed fields / keys / TTL / rendered SAM env (<=4096 bytes)")
    package = Path(__file__).parents[1] / "src" / "scheduled_ingestor"
    if package.joinpath("providers", "open_meteo.yaml").exists():
        raise ValueError("Remove obsolete packaged provider descriptor")
    if any(not package.joinpath(module).is_file() for module in REQUIRED_MODULES):
        raise ValueError("Required core/plugin module missing from source package")
    print("PASS source package readiness; SAM/cfn/build follow locally")
    print("LOCAL ONLY: cannot prove API availability, AWS account, or credentials")
    return config


def verify_package() -> None:
    """Require a code-only SAM package matching current application source.

    Raises:
        ValueError: If modules are missing/stale or the build includes non-source files.
    """
    root = Path(__file__).parents[1]
    source = root / "src" / "scheduled_ingestor"
    built = root / ".aws-sam" / "build" / "FetcherFunction" / "scheduled_ingestor"
    for directory in (source, built):
        if not directory.is_dir():
            raise ValueError("Application package missing; run make build")
        if list(directory.joinpath("providers").glob("*.yaml")):
            raise ValueError("Application package contains obsolete provider descriptor")
        if any(directory.joinpath(module).exists() for module in OBSOLETE_MODULES):
            raise ValueError("Application package contains obsolete modules; remove legacy paths")
        if any(not directory.joinpath(module).is_file() for module in REQUIRED_MODULES):
            raise ValueError("Required core/plugin module missing from application package")
    expected = {item.relative_to(source) for item in source.rglob("*.py")}
    actual = {item.relative_to(built) for item in built.rglob("*.py")}
    if expected != actual or any(
        source.joinpath(item).read_bytes() != built.joinpath(item).read_bytes() for item in expected
    ):
        raise ValueError("SAM package modules differ from current snapshot source; rebuild")
    allowed = {Path("scheduled_ingestor") / item for item in expected} | {Path("requirements.txt")}
    packaged = {
        item.relative_to(built.parent) for item in built.parent.rglob("*") if item.is_file()
    }
    if packaged - allowed:
        raise ValueError("SAM package must be code-only; remove dependencies and non-source files")
    print("PASS SAM package: exact current module bytes; code-only, no dependency distributions")


def env() -> dict[str, str]:
    """Return AWS CLI environment defaults and overrides.

    Returns:
        Environment values used by deployment commands.
    """
    return {
        "AWS_PROFILE": os.environ.get("AWS_PROFILE", "default"),
        "AWS_REGION": os.environ.get("AWS_REGION", "eu-north-1"),
    }


def _redact_command(command: list[str]) -> list[str]:
    redacted: list[str] = []
    in_parameter_overrides = False
    for argument in command:
        if argument == "--parameter-overrides":
            in_parameter_overrides = True
            redacted.append(argument)
        elif in_parameter_overrides and argument.startswith("--"):
            in_parameter_overrides = False
            redacted.append(argument)
        elif in_parameter_overrides and "=" in argument:
            name, _ = argument.split("=", 1)
            redacted.append(f"{name}=<redacted>")
        else:
            redacted.append(argument)
    return redacted


def run(command: list[str], capture: bool = False) -> str:
    """Run a deployment command with configured AWS environment values.

    Args:
        command: Executable and arguments to run.
        capture: Whether to return standard output.

    Returns:
        Captured standard output, or an empty string.

    Raises:
        RuntimeError: If the command exits unsuccessfully.
    """
    print("+", shlex.join(_redact_command(command)))
    try:
        process = subprocess.run(
            command, check=True, text=True, capture_output=capture, env={**os.environ, **env()}
        )
    except subprocess.CalledProcessError as error:
        redacted_command = shlex.join(_redact_command(command))
        raise RuntimeError(
            f"Command failed with exit code {error.returncode}: {redacted_command}"
        ) from None
    return process.stdout.strip() if capture else ""


def _parameter_values(config: dict) -> dict[str, str]:
    """Build unquoted CloudFormation parameter values from validated config.

    Args:
        config: Application configuration mapping.

    Returns:
        CloudFormation parameter names and their string values.

    Raises:
        ValueError: If schedule or runtime values are invalid.
    """
    p = config["provider"]
    s = config["schedule"]
    r = config["runtime"]
    validate_schedule(s)
    validate_runtime(r)
    validate_application(config["application"])
    validate_provider_name(p["name"])
    validate_provider_base_url(p["base_url"])
    validate_provider_options(p.get("options", {}))
    create_provider(p["name"], p["base_url"], p.get("options", {}))
    log = config["logging"]
    validate_logging(log)
    storage = config["storage"]
    alarms = config["alarms"]
    validate_alarms(alarms)
    environment = lambda_environment_values(config)
    validate_lambda_environment_size(config)
    values = {
        "Environment": config["application"]["environment"],
        "ProviderName": p["name"],
        "ApiBaseUrl": p["base_url"],
        "CredentialParameterName": p.get("credential_parameter_name") or "",
        "ProviderOptionsJson": environment["PROVIDER_OPTIONS_JSON"],
        "ScheduleExpression": s["expression"],
        "ScheduleState": "ENABLED" if s["enabled"] else "DISABLED",
        "MaximumRetryAttempts": s["maximum_retry_attempts"],
        "MaximumEventAgeSeconds": s["maximum_event_age_seconds"],
        "MemorySize": r["memory_mb"],
        "FunctionTimeout": r["timeout_seconds"],
        "LogLevel": log["level"],
        "LogRetentionDays": log["retention_days"],
        "DataRetentionDays": storage["ttl"]["retention_days"],
        "EnablePITR": str(storage["point_in_time_recovery"]).lower(),
        "TableName": storage["table_name"],
        "BillingMode": storage["billing_mode"],
        "ReadCapacityUnits": storage["provisioned_throughput"]["read_capacity_units"],
        "WriteCapacityUnits": storage["provisioned_throughput"]["write_capacity_units"],
        "PartitionKeyName": storage["partition_key"]["name"],
        "PartitionKeyType": storage["partition_key"]["type"],
        "SortKeyName": storage["sort_key"]["name"],
        "SortKeyType": storage["sort_key"]["type"],
        "TtlAttributeName": storage["ttl"]["attribute_name"],
        "StorageSchemaJson": environment["STORAGE_SCHEMA_JSON"],
        "EnableAlarms": str(alarms["enabled"]).lower(),
        "AlarmNotificationTopicArn": alarms.get("notification_topic_arn") or "",
    }
    return {key: str(value) for key, value in values.items()}


def parameters(config: dict) -> list[str]:
    """Build SAM-quoted parameter overrides, including explicit empty values.

    Args:
        config: Application configuration mapping.

    Returns:
        Parameter override arguments for SAM's parser, not a shell.

    Raises:
        ValueError: If schedule or runtime values are invalid.
    """
    overrides = []
    for key, value in _parameter_values(config).items():
        escaped = value.replace('"', r"\"").replace("'", r"\'").replace(" ", r"\ ")
        overrides.append(f"{key}='{escaped}'")
    return overrides


def output(stack: str, key: str) -> str:
    """Read one output value from a deployed CloudFormation stack.

    Args:
        stack: CloudFormation stack name.
        key: Output key to retrieve.

    Returns:
        Output value returned by CloudFormation.

    Raises:
        RuntimeError: If the AWS CLI command fails.
        KeyError: If the requested output is absent.
    """
    raw = run(
        ["aws", "cloudformation", "describe-stacks", "--stack-name", stack, "--output", "json"],
        capture=True,
    )
    outputs = json.loads(raw)["Stacks"][0].get("Outputs", [])
    return next(item["OutputValue"] for item in outputs if item["OutputKey"] == key)


def main() -> None:
    """Run the requested configuration or deployment action.

    Returns:
        None.

    Raises:
        ValueError: If configuration validation fails.
        RuntimeError: If a requested operation fails.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "action", choices=["validate", "deploy", "smoke-test", "logs", "delete", "package-check"]
    )
    parser.add_argument("config")
    args = parser.parse_args()
    if args.action == "package-check":
        verify_package()
        return
    config = readiness(args.config) if args.action in {"validate", "deploy"} else load(args.config)
    stack = config["application"]["stack_name"]
    validate_stack_name(stack, "STACK_NAME")

    if args.action == "validate":
        print("Configuration ready (disabled schedule is valid)")
    elif args.action == "deploy":
        run(["aws", "sts", "get-caller-identity", "--query", "Account", "--output", "text"])
        run(["sam", "build"])
        run(
            [
                "find",
                ".aws-sam/build/FetcherFunction",
                "-type",
                "f",
                "-name",
                ".DS_Store",
                "-delete",
            ]
        )
        verify_package()
        run(
            [
                "sam",
                "deploy",
                "--stack-name",
                stack,
                "--resolve-s3",
                "--capabilities",
                "CAPABILITY_IAM",
                "--confirm-changeset",
                "--parameter-overrides",
                *parameters(config),
            ]
        )
    elif args.action == "smoke-test":
        function = output(stack, "FunctionName")
        result = Path(".smoke-response.json")
        run(
            ["aws", "lambda", "invoke", "--function-name", function, "--payload", "{}", str(result)]
        )
        body = json.loads(result.read_text())
        result.unlink(missing_ok=True)
        if body.get("status") != "ok":
            raise RuntimeError(f"Smoke test failed: {body}")
        print(json.dumps(body, indent=2))
    elif args.action == "logs":
        run(["sam", "logs", "--stack-name", stack, "--name", "FetcherFunction", "--tail"])
    elif args.action == "delete":
        answer = input(f"Type stack name '{stack}' to delete it: ")
        if answer != stack:
            raise RuntimeError("Deletion cancelled")
        run(["sam", "delete", "--stack-name", stack, "--no-prompts"])


if __name__ == "__main__":
    try:
        main()
    except (ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1) from error
