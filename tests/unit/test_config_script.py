import ast
import importlib.util
import json
import re
import shlex
import shutil
import subprocess
import sys
import tomllib
import typing
from collections.abc import Callable
from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import ModuleType
from unittest.mock import create_autospec

import pytest

from scheduled_ingestor.core.config import Settings
from scheduled_ingestor.core.http import HttpClient
from scheduled_ingestor.core.models import Observation
from scheduled_ingestor.core.provider import BaseProvider
from scheduled_ingestor.core.repository import ObservationRepository
from scheduled_ingestor.core.storage import TableSchema
from scheduled_ingestor.registry import create_provider

REPOSITORY_ROOT = Path(__file__).parents[2]
CONFIG_PATH = Path(__file__).parents[1] / "fixtures" / "config.yaml"
SCRIPT_PATH = REPOSITORY_ROOT / "scripts" / "config.py"
TEMPLATE_PATH = REPOSITORY_ROOT / "template.yaml"
PYPROJECT_PATH = REPOSITORY_ROOT / "pyproject.toml"
HTTP_CLIENT_PATH = REPOSITORY_ROOT / "src" / "scheduled_ingestor" / "core" / "http.py"


def load_config_script() -> ModuleType:
    spec = importlib.util.spec_from_file_location("config_script", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load config script")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def sam_parser() -> Callable[[list[str]], dict[str, str]]:
    executable = shutil.which("sam")
    if executable is None:
        pytest.skip("SAM CLI is required for parser contract tests")
    launcher = Path(executable).resolve().read_text(encoding="utf-8").splitlines()[0]
    interpreter = launcher.removeprefix("#!")
    if not Path(interpreter).is_file() or not Path(interpreter).name.startswith("python"):
        pytest.skip("SAM parser contract tests require a Python shebang launcher")
    probe = """
import json
import os
import socket
import sys

os.environ["SAM_CLI_TELEMETRY"] = "0"
os.environ["AWS_EC2_METADATA_DISABLED"] = "true"

def deny_network(*args, **kwargs):
    raise AssertionError("Network forbidden in SAM parser contract tests")

socket.socket.connect = deny_network
socket.socket.connect_ex = deny_network
socket.create_connection = deny_network
socket.getaddrinfo = deny_network

from samcli.cli.types import CfnParameterOverridesType

try:
    result = CfnParameterOverridesType().convert(json.load(sys.stdin), None, None)
except Exception as error:
    result = {"error": type(error).__name__}
print(json.dumps(result))
"""

    def parse(arguments: list[str]) -> dict[str, str]:
        result = subprocess.run(
            [interpreter, "-c", probe],
            input=json.dumps(arguments),
            check=True,
            text=True,
            capture_output=True,
        )
        return json.loads(result.stdout)

    return parse


def test_sam_parser_rejects_bare_empty_parameter(
    sam_parser: Callable[[list[str]], dict[str, str]],
) -> None:
    assert sam_parser(["CredentialParameterName="]) == {"error": "BadParameter"}


@pytest.mark.parametrize("optional_value", [None, "", "/example/credential"])
def test_parameters_preserve_optional_values_and_explicit_empty_resets(
    optional_value: str | None,
    sam_parser: Callable[[list[str]], dict[str, str]],
) -> None:
    config_script = load_config_script()
    config = config_script.load(CONFIG_PATH)
    config["provider"]["credential_parameter_name"] = optional_value

    parsed = sam_parser(config_script.parameters(config))

    assert parsed["CredentialParameterName"] == (optional_value or "")
    assert parsed["AlarmNotificationTopicArn"] == ""
    assert parsed["ScheduleExpression"] == config["schedule"]["expression"]
    assert parsed["MemorySize"] == str(config["runtime"]["memory_mb"])
    assert parsed["EnableAlarms"] == "false"
    environment = config_script.lambda_environment_values(config)
    assert parsed["ProviderOptionsJson"] == environment["PROVIDER_OPTIONS_JSON"]
    assert parsed["StorageSchemaJson"] == environment["STORAGE_SCHEMA_JSON"]


@pytest.mark.parametrize(
    "site",
    ["example-site", "example, site", 'example "quoted" site', "example's site", r"example\ site"],
)
def test_parameters_json_round_trips_through_sam_parser(
    site: str,
    sam_parser: Callable[[list[str]], dict[str, str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_script = load_config_script()
    config = config_script.load(CONFIG_PATH)
    environment = config_script.lambda_environment_values(config)
    environment["PROVIDER_OPTIONS_JSON"] = json.dumps({"site": site})
    monkeypatch.setattr(config_script, "lambda_environment_values", lambda _: environment)

    parsed = sam_parser(config_script.parameters(config))

    assert parsed["ProviderOptionsJson"] == environment["PROVIDER_OPTIONS_JSON"]
    assert parsed["StorageSchemaJson"] == environment["STORAGE_SCHEMA_JSON"]


@pytest.fixture(params=["unfinished-root", "completed-root"])
def unfinished_scaffold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> Path:
    config_script = load_config_script()
    completed = config_script.yaml.safe_load(CONFIG_PATH.read_text())
    completed["application"]["stack_name"] = "configured-health-check"
    completed["provider"]["options"]["measurements"] = completed["provider"]["options"][
        "measurements"
    ][:1]
    scaffold = deepcopy(completed)
    scaffold["application"]["stack_name"] = None
    scaffold["provider"]["options"]["measurements"] = []
    root = tmp_path / "root"
    root.mkdir()
    root_config = root / "config.yaml"
    root_config.write_text(json.dumps(completed if request.param == "completed-root" else scaffold))
    monkeypatch.setitem(globals(), "REPOSITORY_ROOT", root)
    if request.param == "completed-root":
        config_script.load(root_config)
    scaffold_path = tmp_path / "unfinished.yaml"
    scaffold_path.write_text(json.dumps(scaffold))
    return scaffold_path


def test_parameters_default_alarms_are_disabled() -> None:
    config_script = load_config_script()

    parameters = config_script.parameters(config_script.load(CONFIG_PATH))

    assert "EnableAlarms='false'" in parameters


def test_parameters_explicitly_enabled_alarms_are_enabled() -> None:
    config_script = load_config_script()
    config = config_script.load(CONFIG_PATH)
    config["alarms"]["enabled"] = True

    parameters = config_script.parameters(config)

    assert "EnableAlarms='true'" in parameters


def test_parameters_reject_non_boolean_alarm_enabled_value() -> None:
    config_script = load_config_script()
    config = config_script.load(CONFIG_PATH)
    config["alarms"]["enabled"] = "false"

    with pytest.raises(ValueError, match=r"alarms\.enabled"):
        config_script.parameters(config)


def test_parameters_include_deployable_storage_settings_only(
    sam_parser: Callable[[list[str]], dict[str, str]],
) -> None:
    config_script = load_config_script()
    config = config_script.load(CONFIG_PATH)

    parameters = config_script.parameters(config)

    parsed = sam_parser(parameters)
    assert parsed["TableName"] == "serverless-api-ingestor-dev-observations"
    assert parsed["BillingMode"] == "PROVISIONED"
    assert parsed["ReadCapacityUnits"] == "1"
    assert parsed["WriteCapacityUnits"] == "1"
    assert parsed["PartitionKeyName"] == "date"
    assert parsed["PartitionKeyType"] == "N"
    assert parsed["SortKeyName"] == "ts"
    assert parsed["SortKeyType"] == "N"
    assert parsed["TtlAttributeName"] == "expts"
    schema = json.loads(parsed["StorageSchemaJson"])
    assert schema["partition_key"] == {"name": "date", "type": "N"}
    assert schema["sort_key"] == {"name": "ts", "type": "N"}
    assert schema["ttl"] == {"attribute_name": "expts", "retention_days": 90}
    assert schema["fields"] == [
        {"name": "date", "type": "N"},
        {"name": "ts", "type": "N"},
        {"name": "expts", "type": "N"},
        {"name": "temperature", "type": "N"},
        {"name": "apparent", "type": "N"},
    ]
    assert (
        config_script.runtime_storage_schema(config["storage"], config["provider"]["options"])[
            "fields"
        ]
        == schema["fields"]
    )
    assert TableSchema.from_json(json.dumps(schema)).to_mapping() == schema
    assert not any(parameter.startswith("Fields=") for parameter in parameters)


def test_template_passes_runtime_storage_schema_to_lambda() -> None:
    template = TEMPLATE_PATH.read_text()

    assert "StorageSchemaJson:\n    Type: String" in template
    assert "STORAGE_SCHEMA_JSON: !Ref StorageSchemaJson" in template


def test_template_restricts_credential_parameter_name_to_empty_or_ssm_path() -> None:
    config_script = load_config_script()
    template = TEMPLATE_PATH.read_text()

    assert (
        "CredentialParameterName:\n"
        "    Type: String\n"
        '    Default: ""\n'
        "    MaxLength: 2048\n"
        f'    AllowedPattern: "{config_script.SSM_PARAMETER_PATTERN}"'
    ) in template
    assert re.fullmatch(config_script.SSM_PARAMETER_PATTERN, "")
    assert re.fullmatch(config_script.SSM_PARAMETER_PATTERN, "/" + "a" * 2047)


def test_template_schedule_pattern_matches_config_script() -> None:
    config_script = load_config_script()
    template = TEMPLATE_PATH.read_text()

    assert f"AllowedPattern: '{config_script.SCHEDULE_EXPRESSION_PATTERN.pattern}'" in template
    assert "ScheduleExpression:\n    Type: String\n    MaxLength: 256" in template


@pytest.mark.parametrize(
    "parameter",
    [
        "/aws",
        "/aws/credential",
        "/AWS",
        "/AWS/credential",
        "/ssm",
        "/ssm/credential",
        "/SSM",
        "/sSm/credential",
        "/a/" + "/".join("level" + str(index) for index in range(15)),
        "/a//b",
        "/a/",
    ],
)
def test_ssm_parameter_name_rejects_invalid_hierarchy(parameter: str) -> None:
    config_script = load_config_script()

    assert re.fullmatch(config_script.SSM_PARAMETER_PATTERN, parameter) is None


def test_ssm_parameter_name_accepts_fifteen_levels() -> None:
    config_script = load_config_script()
    parameter = "/" + "/".join("level" + str(index) for index in range(15))

    assert re.fullmatch(config_script.SSM_PARAMETER_PATTERN, parameter)


@pytest.mark.parametrize("parameter", ["/aws-credential", "/ssm_parameter", "/Awsian/credential"])
def test_ssm_parameter_name_accepts_non_reserved_prefix(parameter: str) -> None:
    config_script = load_config_script()

    assert re.fullmatch(config_script.SSM_PARAMETER_PATTERN, parameter)


def test_load_accepts_max_length_ssm_parameter_name(tmp_path: Path) -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    parameter = "/" + "a" * 2047
    config["provider"]["credential_parameter_name"] = parameter
    valid_config_path = tmp_path / "config.yaml"
    valid_config_path.write_text(json.dumps(config))

    assert (
        config_script.load(valid_config_path)["provider"]["credential_parameter_name"] == parameter
    )


@pytest.mark.parametrize(
    ("section", "field", "value", "message"),
    [
        ("schedule", "expression", [], r"schedule\.expression"),
        ("schedule", "maximum_retry_attempts", True, r"schedule\.maximum_retry_attempts"),
        ("schedule", "maximum_retry_attempts", 6, r"schedule\.maximum_retry_attempts"),
        ("schedule", "maximum_event_age_seconds", {}, r"schedule\.maximum_event_age_seconds"),
        ("schedule", "maximum_event_age_seconds", 59, r"schedule\.maximum_event_age_seconds"),
        ("runtime", "memory_mb", False, r"runtime\.memory_mb"),
        ("runtime", "memory_mb", 10241, r"runtime\.memory_mb"),
        ("runtime", "timeout_seconds", [], r"runtime\.timeout_seconds"),
        ("runtime", "timeout_seconds", 0, r"runtime\.timeout_seconds"),
    ],
)
def test_load_rejects_invalid_template_parameter_values(
    tmp_path: Path, section: str, field: str, value: object, message: str
) -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    config[section][field] = value
    invalid_config_path = tmp_path / "config.yaml"
    invalid_config_path.write_text(json.dumps(config))

    with pytest.raises(ValueError, match=message):
        config_script.load(invalid_config_path)


def test_parameters_rejects_invalid_runtime_value() -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    config["runtime"]["memory_mb"] = []

    with pytest.raises(ValueError, match=r"runtime\.memory_mb"):
        config_script.parameters(config)


def test_template_requires_non_empty_https_host_for_api_base_url() -> None:
    config_script = load_config_script()
    template = TEMPLATE_PATH.read_text()
    allowed_pattern = config_script.BASE_URL_PATTERN

    assert f'AllowedPattern: "{allowed_pattern}"' in template
    accepted_urls = [
        "https://api.example.com",
        "https://api.example.com:8443/v1/rates?symbol=BTC-EUR",
        "https://api.example.com:65535/v1/rates",
        "https://127.0.0.1:8443/rates?active=true",
        "https://localhost/path?query=value",
    ]
    rejected_urls = [
        "https://",
        "https://user:pass@api.example.com",
        "https://api.example.com:bad/path",
        "https://api.example.com:0/path",
        "https://api.example.com:65536/path",
        "https://api.example.com:99999/path",
        "https://api.example.com/rates#latest",
    ]

    assert all(re.fullmatch(allowed_pattern, url) for url in accepted_urls)
    assert not any(re.fullmatch(allowed_pattern, url) for url in rejected_urls)


def test_load_rejects_malformed_yaml(tmp_path: Path) -> None:
    config_script = load_config_script()
    invalid_config_path = tmp_path / "config.yaml"
    invalid_config_path.write_text("provider: [\n")

    with pytest.raises(ValueError, match="Invalid YAML"):
        config_script.load(invalid_config_path)


def test_load_rejects_unknown_nested_config_key(tmp_path: Path) -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    config["provider"]["unexpected"] = "value"
    invalid_config_path = tmp_path / "config.yaml"
    invalid_config_path.write_text(json.dumps(config))

    with pytest.raises(ValueError, match=r"Invalid provider.*unknown"):
        config_script.load(invalid_config_path)


def test_load_rejects_malformed_nested_config_section(tmp_path: Path) -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    config["logging"] = []
    invalid_config_path = tmp_path / "config.yaml"
    invalid_config_path.write_text(json.dumps(config))

    with pytest.raises(ValueError, match="logging must be a mapping"):
        config_script.load(invalid_config_path)


@pytest.mark.parametrize(
    "base_url",
    [
        "https://",
        "https:///rates",
        "https://user:pass@api.example.com",
        "https://api.example.com:bad/path",
        "https://api.example.com/rates#latest",
        "http://api.example.com",
    ],
)
def test_load_rejects_provider_base_url_without_https_host(tmp_path: Path, base_url: str) -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    config["provider"]["base_url"] = base_url
    invalid_config_path = tmp_path / "config.yaml"
    invalid_config_path.write_text(json.dumps(config))

    with pytest.raises(ValueError, match=r"provider\.base_url must use HTTPS"):
        config_script.load(invalid_config_path)


@pytest.mark.parametrize(
    "base_url",
    [
        "https://api.example.com/v1/rates",
        "https://api.example.com:8443/v1/rates?symbol=BTC-EUR",
        "https://127.0.0.1:8443/rates?active=true",
        "https://localhost/path?query=value",
    ],
)
def test_generic_validation_accepts_provider_base_url_with_path_or_query(base_url: str) -> None:
    config_script = load_config_script()

    config_script.validate_provider_base_url(base_url)


@pytest.mark.parametrize(
    "expression",
    [
        "rate(1 minute)",
        "rate(5 minutes)",
        "rate(10 minutes)",
        "rate(1 hour)",
        "rate(2 hours)",
        "rate(1 day)",
        "rate(3 days)",
        "cron(0 12 * * ? *)",
        "cron(0 9-17 ? * MON-FRI *)",
        "cron(0 8,12,16 * * ? *)",
        "cron(0/15 * * * ? *)",
        "cron(0 0 L * ? *)",
        "cron(0 0 3W * ? *)",
        "cron(0 0 ? * 6L *)",
        "cron(0 0 ? * 2#1 *)",
    ],
)
def test_load_accepts_supported_schedule_expressions(tmp_path: Path, expression: str) -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    config["schedule"]["expression"] = expression
    valid_config_path = tmp_path / "config.yaml"
    valid_config_path.write_text(json.dumps(config))

    assert config_script.load(valid_config_path)["schedule"]["expression"] == expression


def test_load_accepts_256_character_schedule_expression(tmp_path: Path) -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    expression = f"cron({','.join(['0'] * 120)} 12 * * ? *)"
    config["schedule"]["expression"] = expression
    valid_config_path = tmp_path / "config.yaml"
    valid_config_path.write_text(json.dumps(config))

    assert len(expression) == 256
    assert config_script.load(valid_config_path)["schedule"]["expression"] == expression


def test_load_rejects_257_character_schedule_expression(tmp_path: Path) -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    expression = f"cron({','.join(['0'] * 120)} 12 1W * ? *)"
    config["schedule"]["expression"] = expression
    invalid_config_path = tmp_path / "config.yaml"
    invalid_config_path.write_text(json.dumps(config))

    assert len(expression) == 257
    with pytest.raises(ValueError, match=r"schedule\.expression"):
        config_script.load(invalid_config_path)


@pytest.mark.parametrize(
    "expression",
    [
        "rate(1 minutes)",
        "rate(2 minute)",
        "rate(0 minutes)",
        "rate(5 weeks)",
        "rate(5 minutes",
        "cron()",
        "cron( )",
        "cron(0 12 * * *)",
        "cron(60 12 * * ? *)",
        "cron(0 24 * * ? *)",
        "cron(0 12 32 * ? *)",
        "cron(0 12 * 13 ? *)",
        "cron(0 12 * * 8 *)",
        "cron(0 12 * * ? 1969)",
        "cron(0 12 * * ? abc)",
        "cron(0 12 * * ? *!)",
        "cron(0 12 * * * *)",
        "cron(0 12 ? * 2#1,4#2 *)",
    ],
)
def test_load_rejects_invalid_schedule_expressions(tmp_path: Path, expression: str) -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    config["schedule"]["expression"] = expression
    invalid_config_path = tmp_path / "config.yaml"
    invalid_config_path.write_text(json.dumps(config))

    with pytest.raises(ValueError, match=r"schedule\.expression"):
        config_script.load(invalid_config_path)


@pytest.mark.parametrize(
    "expression",
    [
        "cron(9-3 12 * * ? *)",
        "cron(0 17-9 * * ? *)",
        "cron(0 12 31-1 * ? *)",
        "cron(0 12 * DEC-JAN ? *)",
        "cron(0 12 ? * FRI-MON *)",
        "cron(0 12 * * ? 2025-2020)",
    ],
)
def test_load_rejects_descending_cron_ranges(tmp_path: Path, expression: str) -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    config["schedule"]["expression"] = expression
    invalid_config_path = tmp_path / "config.yaml"
    invalid_config_path.write_text(json.dumps(config))

    with pytest.raises(ValueError, match=r"schedule\.expression.*descending range"):
        config_script.load(invalid_config_path)


@pytest.mark.parametrize(
    "expression",
    [
        "cron(3-9 12 * * ? *)",
        "cron(0 9-17 * * ? *)",
        "cron(0 12 1-31 * ? *)",
        "cron(0 12 * JAN-DEC ? *)",
        "cron(0 12 ? * MON-FRI *)",
        "cron(0 12 * * ? 1970-2099)",
    ],
)
def test_load_accepts_ascending_cron_ranges(tmp_path: Path, expression: str) -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    config["schedule"]["expression"] = expression
    valid_config_path = tmp_path / "config.yaml"
    valid_config_path.write_text(json.dumps(config))

    assert config_script.load(valid_config_path)["schedule"]["expression"] == expression


def test_load_rejects_oversized_provider_options_payload(tmp_path: Path) -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    config["provider"]["options"]["payload"] = "x" * 10000
    invalid_config_path = tmp_path / "config.yaml"
    invalid_config_path.write_text(json.dumps(config))

    with pytest.raises(ValueError, match="Lambda environment variables require"):
        config_script.load(invalid_config_path)


def test_load_rejects_oversized_storage_schema_payload(tmp_path: Path) -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    config["provider"]["options"]["measurements"].extend(
        {"field": "temperature_2m", "name": f"field_{index}", "type": "N", "unit": "degC"}
        for index in range(200)
    )
    invalid_config_path = tmp_path / "config.yaml"
    invalid_config_path.write_text(json.dumps(config))

    with pytest.raises(ValueError, match="Lambda environment variables require"):
        config_script.load(invalid_config_path)


@pytest.mark.parametrize(
    ("section", "field", "value", "message"),
    [
        ("application", "stack_name", "1-invalid", r"application\.stack_name"),
        ("application", "environment", "DEV", r"application\.environment"),
        ("provider", "name", "INVALID", r"provider\.name"),
        ("logging", "level", "TRACE", r"logging\.level"),
        ("alarms", "notification_topic_arn", "not-an-arn", r"alarms\.notification_topic_arn"),
    ],
)
def test_load_rejects_nested_scalars_outside_template_constraints(
    tmp_path: Path, section: str, field: str, value: object, message: str
) -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    config[section][field] = value
    invalid_config_path = tmp_path / "config.yaml"
    invalid_config_path.write_text(json.dumps(config))

    with pytest.raises(ValueError, match=message):
        config_script.load(invalid_config_path)


def test_load_accepts_55_character_stack_name(tmp_path: Path) -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    config["application"]["stack_name"] = "a" + "b" * 54
    valid_config_path = tmp_path / "config.yaml"
    valid_config_path.write_text(json.dumps(config))

    assert len(config_script.load(valid_config_path)["application"]["stack_name"]) == 55


def test_load_rejects_56_character_stack_name(tmp_path: Path) -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    config["application"]["stack_name"] = "a" + "b" * 55
    invalid_config_path = tmp_path / "config.yaml"
    invalid_config_path.write_text(json.dumps(config))

    with pytest.raises(ValueError, match=r"application\.stack_name"):
        config_script.load(invalid_config_path)


def test_load_accepts_synthetic_alarm_topic_arn(tmp_path: Path) -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    config["alarms"]["notification_topic_arn"] = "arn:aws:sns:eu-north-1:000000000000:notifications"
    valid_config_path = tmp_path / "config.yaml"
    valid_config_path.write_text(json.dumps(config))

    assert config_script.load(valid_config_path)["alarms"]["notification_topic_arn"].startswith(
        "arn:aws:sns:"
    )


def test_load_rejects_unsupported_config_version_key(tmp_path: Path) -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    config["version"] = True
    invalid_config_path = tmp_path / "config.yaml"
    invalid_config_path.write_text(json.dumps(config))

    with pytest.raises(ValueError, match=r"unknown=.*version"):
        config_script.load(invalid_config_path)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("enabled", "true", r"schedule\.enabled"),
        ("flexible_time_window", "false", r"schedule\.flexible_time_window"),
        ("flexible_time_window", True, r"schedule\.flexible_time_window"),
    ],
)
def test_load_rejects_invalid_schedule_scalars(
    tmp_path: Path, field: str, value: object, message: str
) -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    config["schedule"][field] = value
    invalid_config_path = tmp_path / "config.yaml"
    invalid_config_path.write_text(json.dumps(config))

    with pytest.raises(ValueError, match=message):
        config_script.load(invalid_config_path)


def test_load_rejects_unsupported_runtime_architecture(tmp_path: Path) -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    config["runtime"]["architecture"] = "x86_64"
    invalid_config_path = tmp_path / "config.yaml"
    invalid_config_path.write_text(json.dumps(config))

    with pytest.raises(ValueError, match=r"runtime\.architecture"):
        config_script.load(invalid_config_path)


def test_load_rejects_overlong_ssm_parameter_name(tmp_path: Path) -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    config["provider"]["credential_parameter_name"] = "/" + "a" * 2048
    invalid_config_path = tmp_path / "config.yaml"
    invalid_config_path.write_text(json.dumps(config))

    with pytest.raises(ValueError, match="Invalid SSM parameter name"):
        config_script.load(invalid_config_path)


def test_load_rejects_unhashable_logging_retention_days(tmp_path: Path) -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    config["logging"]["retention_days"] = []
    invalid_config_path = tmp_path / "config.yaml"
    invalid_config_path.write_text(json.dumps(config))

    with pytest.raises(ValueError, match=r"logging\.retention_days"):
        config_script.load(invalid_config_path)


@pytest.mark.parametrize("reserved_name", ["date", "ts", "expts"])
def test_selected_fields_reject_key_or_ttl_collision(reserved_name: str) -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    config["provider"]["options"]["measurements"][0]["name"] = reserved_name

    with pytest.raises(ValueError, match="unique"):
        config_script.runtime_storage_schema(config["storage"], config["provider"]["options"])


def test_load_rejects_invalid_storage_throughput() -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    config["storage"]["provisioned_throughput"]["write_capacity_units"] = 0

    try:
        config_script.validate_storage(config["storage"])
    except ValueError as error:
        assert "write_capacity_units" in str(error)
    else:
        raise AssertionError("Expected invalid throughput to be rejected")


def test_load_rejects_invalid_storage_schema() -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    config["storage"]["partition_key"]["type"] = "INVALID"

    try:
        config_script.validate_storage(config["storage"])
    except ValueError as error:
        assert "partition_key.type" in str(error)
    else:
        raise AssertionError("Expected invalid key schema to be rejected")


def test_load_rejects_unhashable_storage_key_type(tmp_path: Path) -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    config["storage"]["partition_key"]["type"] = []
    invalid_config_path = tmp_path / "config.yaml"
    invalid_config_path.write_text(json.dumps(config))

    with pytest.raises(ValueError, match=r"partition_key.type"):
        config_script.load(invalid_config_path)


def test_validate_storage_rejects_duplicate_key_name() -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    config["storage"]["partition_key"]["name"] = config["storage"]["sort_key"]["name"]

    with pytest.raises(ValueError, match="must differ"):
        config_script.validate_storage(config["storage"])


def test_load_rejects_provider_options_with_non_string_value(tmp_path: Path) -> None:
    config_script = load_config_script()
    config = deepcopy(config_script.load(CONFIG_PATH))
    config["provider"]["options"] = {"latitude": ["0.0"]}
    invalid_config_path = tmp_path / "config.yaml"
    invalid_config_path.write_text(json.dumps(config))

    with pytest.raises(ValueError, match=r"provider.options"):
        config_script.load(invalid_config_path)


def test_alarm_resources_are_conditioned_on_opt_in_parameter() -> None:
    template = TEMPLATE_PATH.read_text()

    assert 'EnableAlarms:\n    Type: String\n    Default: "false"' in template
    assert 'AlarmsEnabled: !Equals [!Ref EnableAlarms, "true"]' in template
    assert template.count("    Condition: AlarmsEnabled") == 3


def test_package_metadata_uses_public_project_name() -> None:
    assert 'name = "serverless-api-ingestor"' in PYPROJECT_PATH.read_text()


def test_dependencies_are_local_only_and_sam_manifest_is_empty() -> None:
    project = tomllib.loads(PYPROJECT_PATH.read_text())["project"]
    manifest = (REPOSITORY_ROOT / "src" / "requirements.txt").read_text()

    assert project["dependencies"] == []
    dependencies = project["optional-dependencies"]["dev"]
    assert {"boto3>=1.35,<2", "botocore>=1.35,<2", "urllib3>=2.2,<3"} <= set(dependencies)
    assert not any("powertools" in dependency for dependency in dependencies)
    assert not any(line.strip() and not line.startswith("#") for line in manifest.splitlines())


def test_production_imports_use_only_stdlib_project_or_runtime_sdk() -> None:
    imported = set()
    for module in (REPOSITORY_ROOT / "src" / "scheduled_ingestor").rglob("*.py"):
        for node in ast.walk(ast.parse(module.read_text())):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])

    assert imported - sys.stdlib_module_names == {
        "boto3",
        "botocore",
        "scheduled_ingestor",
        "urllib3",
    }


def test_run_redacts_parameter_override_values_but_preserves_command(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config_script = load_config_script()
    config = config_script.load(CONFIG_PATH)
    config["provider"]["base_url"] = "https://api.example.com"
    config["provider"]["options"]["site"] = "secret"
    command = [
        "sam",
        "deploy",
        "--parameter-overrides",
        *config_script.parameters(config),
        "--region",
        "us-east-1",
    ]
    calls: list[list[str]] = []

    def fake_run(actual_command: list[str], **_: object) -> object:
        calls.append(actual_command)
        return subprocess.CompletedProcess(actual_command, 0, stdout="", stderr="")

    monkeypatch.setattr(config_script.subprocess, "run", fake_run)

    config_script.run(command)

    assert calls == [command]
    logged = capsys.readouterr().out
    assert "ApiBaseUrl=<redacted>" in logged
    assert "ProviderOptionsJson=<redacted>" in logged
    assert "CredentialParameterName=<redacted>" in logged
    assert "AlarmNotificationTopicArn=<redacted>" in logged
    assert "--region us-east-1" in logged
    assert "api.example.com" not in logged
    assert "secret" not in logged


def test_run_redacts_failed_subprocess_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_script = load_config_script()
    secret = "super-secret-value"
    parameter = "/example/credential"
    config = config_script.load(CONFIG_PATH)
    config["provider"]["credential_parameter_name"] = parameter
    config["provider"]["options"]["site"] = secret
    command = [
        "sam",
        "deploy",
        "--parameter-overrides",
        *config_script.parameters(config),
    ]

    def fail_run(actual_command: list[str], **_: object) -> object:
        raise subprocess.CalledProcessError(1, actual_command)

    monkeypatch.setattr(config_script.subprocess, "run", fail_run)

    with pytest.raises(RuntimeError) as raised:
        config_script.run(command)

    message = str(raised.value)
    assert secret not in message
    assert parameter not in message
    assert "Command failed with exit code 1" in message
    assert "sam deploy --parameter-overrides" in message
    assert "CredentialParameterName=<redacted>" in message


def test_cli_rejects_invalid_stack_name_override_without_aws_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_script = load_config_script()
    monkeypatch.setenv("STACK_NAME", "a" + "b" * 55)

    def fail_if_called(*_: object, **__: object) -> object:
        raise AssertionError("AWS command should not run")

    monkeypatch.setattr(config_script, "run", fail_if_called)
    monkeypatch.setattr(sys, "argv", [str(SCRIPT_PATH), "validate", str(CONFIG_PATH)])

    with pytest.raises(ValueError, match=r"STACK_NAME"):
        config_script.main()


def test_default_stack_is_public_and_weather_needs_no_credential() -> None:
    config = load_config_script().load(CONFIG_PATH)

    assert config["application"]["stack_name"] == "serverless-api-ingestor-dev"
    assert config["provider"]["credential_parameter_name"] is None


def test_isolated_unfinished_scaffold_rejects_missing_stack_name(
    unfinished_scaffold: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("STACK_NAME", raising=False)

    config_script = load_config_script()
    scaffold = config_script.yaml.safe_load(unfinished_scaffold.read_text())
    assert scaffold["schedule"]["enabled"] is False
    assert scaffold["provider"]["options"]["measurements"] == []
    with pytest.raises(ValueError, match=r"application.stack_name"):
        config_script.load(unfinished_scaffold)


def test_default_table_uses_effective_stack_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STACK_NAME", "weather-alternate-dev")

    config = load_config_script().load(CONFIG_PATH)

    assert config["application"]["stack_name"] == "weather-alternate-dev"
    assert config["storage"]["table_name"] == "weather-alternate-dev-observations"


def test_explicit_table_override_warns_and_preserves_table(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("STACK_NAME", "weather-alternate-dev")
    config_path = tmp_path / "config.yaml"
    completed = load_config_script().yaml.safe_load(CONFIG_PATH.read_text())
    completed["storage"]["table_name"] = "example-explicit-observations"
    config_path.write_text(json.dumps(completed))

    config = load_config_script().load(config_path)

    assert config["storage"]["table_name"] == "example-explicit-observations"
    assert capsys.readouterr().err == (
        "Warning: explicit storage.table_name disables stack-derived table isolation\n"
    )


@pytest.mark.parametrize("name", ["missing", "crypto"])
def test_load_rejects_unregistered_provider(tmp_path: Path, name: str) -> None:
    config_script = load_config_script()
    config_path = tmp_path / "config.yaml"
    config = config_script.yaml.safe_load(CONFIG_PATH.read_text())
    config["provider"]["name"] = name
    config_path.write_text(json.dumps(config))

    with pytest.raises(ValueError, match="Unknown provider"):
        config_script.load(config_path)


@pytest.mark.parametrize(
    "options",
    [
        {"latitude": "90.1"},
        {"longitude": "-180.1"},
        {"api_key": "SYNTHETIC_NOT_A_SECRET"},
    ],
)
def test_load_rejects_invalid_weather_options(tmp_path: Path, options: dict[str, str]) -> None:
    config_script = load_config_script()
    config_path = tmp_path / "config.yaml"
    config = config_script.yaml.safe_load(CONFIG_PATH.read_text())
    config["provider"]["options"].update(options)
    config_path.write_text(json.dumps(config))

    with pytest.raises(ValueError, match=r"provider\.options"):
        config_script.load(config_path)


def test_runtime_telemetry_uses_public_project_name() -> None:
    template = TEMPLATE_PATH.read_text()

    assert "POWERTOOLS_" not in template
    assert "LogFormat: JSON" in template
    assert "ApplicationLogLevel: !FindInMap [LambdaLogLevels, !Ref LogLevel, Level]" in template
    assert "serverless-api-ingestor/0.1" in HTTP_CLIENT_PATH.read_text()


def test_native_lambda_log_levels_preserve_configured_python_levels() -> None:
    config_script = load_config_script()
    template = config_script.yaml.load(
        TEMPLATE_PATH.read_text(), Loader=config_script.TemplateLoader
    )

    assert template["Mappings"]["LambdaLogLevels"] == {
        "DEBUG": {"Level": "DEBUG"},
        "INFO": {"Level": "INFO"},
        "WARNING": {"Level": "WARN"},
        "ERROR": {"Level": "ERROR"},
        "CRITICAL": {"Level": "FATAL"},
    }


def test_lambda_handler_retains_internal_import_package() -> None:
    template = TEMPLATE_PATH.read_text()

    assert "Handler: scheduled_ingestor.handler.lambda_handler" in template


def test_logs_uses_logical_resource_id_without_output_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_script = load_config_script()
    run = create_autospec(config_script.run, return_value="")
    output = create_autospec(
        config_script.output, side_effect=AssertionError("Logs queried physical output")
    )
    subprocess_run = create_autospec(
        config_script.subprocess.run, side_effect=AssertionError("Logs ran subprocess")
    )
    monkeypatch.setenv("STACK_NAME", "example-logs")
    monkeypatch.setattr(config_script, "run", run)
    monkeypatch.setattr(config_script, "output", output)
    monkeypatch.setattr(config_script.subprocess, "run", subprocess_run)
    monkeypatch.setattr(sys, "argv", [str(SCRIPT_PATH), "logs", str(CONFIG_PATH)])

    config_script.main()

    run.assert_called_once_with(
        ["sam", "logs", "--stack-name", "example-logs", "--name", "FetcherFunction", "--tail"]
    )
    output.assert_not_called()
    subprocess_run.assert_not_called()


def test_cli_rejects_removed_export_action() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "export", str(CONFIG_PATH)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "invalid choice: 'export'" in result.stderr


def test_unfinished_scaffold_blocks_deployment_subprocess(
    unfinished_scaffold: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_script = load_config_script()
    monkeypatch.delenv("STACK_NAME", raising=False)
    monkeypatch.setattr(sys, "argv", [str(SCRIPT_PATH), "deploy", str(unfinished_scaffold)])

    def fail_if_called(*_: object, **__: object) -> None:
        raise AssertionError("Deployment subprocess ran before readiness")

    monkeypatch.setattr(config_script.subprocess, "run", fail_if_called)
    with pytest.raises(ValueError, match=r"application\.stack_name"):
        config_script.main()


@pytest.mark.parametrize("target", ["deploy", "smoke-test", "logs", "delete"])
def test_make_cloud_commands_ignore_config_override(target: str) -> None:
    outputs = []
    for override in ([], ["CONFIG=tests/fixtures/config.yaml"]):
        result = subprocess.run(
            ["make", "--no-print-directory", "--dry-run", target, *override],
            cwd=REPOSITORY_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        outputs.append(result.stdout)
    assert outputs[0] == outputs[1]
    commands = [
        shlex.split(line)[1:] for line in outputs[1].splitlines() if "scripts/config.py" in line
    ]
    actions = ["validate", "package-check", "deploy"] if target == "deploy" else [target]
    assert commands == [["scripts/config.py", action, "config.yaml"] for action in actions]


@pytest.mark.parametrize("target", ["validate", "build"])
def test_make_local_commands_preserve_config_override(target: str) -> None:
    result = subprocess.run(
        ["make", "--no-print-directory", "--dry-run", target, f"CONFIG={CONFIG_PATH}"],
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    commands = [
        shlex.split(line)[1:] for line in result.stdout.splitlines() if "scripts/config.py" in line
    ]
    actions = ["validate", "package-check"] if target == "validate" else ["package-check"]
    assert commands == [["scripts/config.py", action, str(CONFIG_PATH)] for action in actions]


@pytest.mark.parametrize("venv", [".venv", "custom-venv"])
def test_make_exports_virtualenv_python_path(venv: str) -> None:
    result = subprocess.run(
        [
            "make",
            "--no-print-directory",
            "-f",
            "Makefile",
            "-f",
            "-",
            "print-runtime-path",
            f"VENV={venv}",
        ],
        input="print-runtime-path:\n\t@printf '%s\\n' \"$$PATH\"\n",
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.stdout.strip().split(":")[0] == str(REPOSITORY_ROOT / venv / "bin")


def test_make_bootstrap_seeds_pip_for_sam() -> None:
    result = subprocess.run(
        ["make", "--no-print-directory", "--dry-run", "bootstrap"],
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    assert [shlex.split(line) for line in result.stdout.splitlines()] == [
        ["uv", "sync", "--locked", "--extra", "dev", "--python", "3.13"],
        [".venv/bin/python", "-m", "ensurepip"],
    ]


@pytest.fixture
def offline_readiness_script(
    monkeypatch: pytest.MonkeyPatch,
) -> ModuleType:
    config_script = load_config_script()

    def fail_if_called(*_: object, **__: object) -> None:
        raise AssertionError("Local readiness made external call")

    monkeypatch.setattr(config_script.subprocess, "run", fail_if_called)
    monkeypatch.setattr("botocore.session.Session.create_client", fail_if_called)
    monkeypatch.setattr(
        config_script.HttpClient,
        "get_json",
        create_autospec(HttpClient.get_json, side_effect=fail_if_called),
    )
    return config_script


def test_completed_fixture_readiness_never_calls_network_or_aws(
    offline_readiness_script: ModuleType,
) -> None:
    config = offline_readiness_script.readiness(CONFIG_PATH)
    assert config["schedule"]["enabled"] is False


@pytest.mark.parametrize("keyword_url", [False, True], ids=["positional", "keyword"])
def test_readiness_accepts_positional_and_keyword_url(
    keyword_url: bool, monkeypatch: pytest.MonkeyPatch, offline_readiness_script: ModuleType
) -> None:
    class RequestProvider(BaseProvider):
        @typing.override
        def fetch(self, client: HttpClient, credential: str | None) -> object:
            assert credential is None
            if keyword_url:
                return client.get_json(url="https://example.com/current")
            return client.get_json("https://example.com/current")

        @typing.override
        def normalize(self, payload: object, observed_at: datetime) -> list[Observation]:
            raise AssertionError("Readiness must not normalize payloads")

    config_script = offline_readiness_script
    monkeypatch.setattr(config_script, "create_provider", lambda *args: RequestProvider())

    config = config_script.readiness(CONFIG_PATH)

    assert config["schedule"]["enabled"] is False


@pytest.mark.parametrize(
    ("arguments", "keywords", "message"),
    [
        pytest.param((), {}, r"valid HttpClient\.get_json call", id="missing-url"),
        pytest.param(
            ("https://example.com/current",),
            {"url": "https://example.com/current"},
            r"valid HttpClient\.get_json call",
            id="duplicate-url",
        ),
        pytest.param(
            ("https://example.com/current",),
            {"timeout": 1},
            r"valid HttpClient\.get_json call",
            id="unexpected-keyword",
        ),
        pytest.param(
            ("https://example.com/current", None, "extra"),
            {},
            r"valid HttpClient\.get_json call",
            id="excess-positional-arguments",
        ),
        pytest.param(("http://example.com/current",), {}, "must use HTTPS", id="positional-http"),
        pytest.param(
            (), {"url": "http://example.com/current"}, "must use HTTPS", id="keyword-http"
        ),
        pytest.param((), {"url": None}, "must use HTTPS", id="invalid-url-type"),
    ],
)
def test_readiness_rejects_invalid_http_calls(
    arguments: tuple[object, ...],
    keywords: dict[str, object],
    message: str,
    monkeypatch: pytest.MonkeyPatch,
    offline_readiness_script: ModuleType,
) -> None:
    provider = create_autospec(BaseProvider, instance=True)
    provider.fetch.side_effect = lambda client, credential: client.get_json(*arguments, **keywords)
    monkeypatch.setattr(offline_readiness_script, "create_provider", lambda *args: provider)

    with pytest.raises(ValueError, match=message):
        offline_readiness_script.readiness(CONFIG_PATH)


@pytest.mark.parametrize("request_count", [0, 2], ids=["no-request", "multiple-requests"])
def test_readiness_rejects_request_count_other_than_one(
    request_count: int,
    monkeypatch: pytest.MonkeyPatch,
    offline_readiness_script: ModuleType,
) -> None:
    provider = create_autospec(BaseProvider, instance=True)
    provider.fetch.side_effect = lambda client, credential: [
        client.get_json("https://example.com/current") for _ in range(request_count)
    ]
    monkeypatch.setattr(offline_readiness_script, "create_provider", lambda *args: provider)

    with pytest.raises(ValueError, match="must construct one bounded HTTPS request"):
        offline_readiness_script.readiness(CONFIG_PATH)


@pytest.mark.parametrize(
    "credential_config",
    [
        pytest.param({"credential_parameter_name": "/example/credential"}, id="configured"),
        pytest.param({"credential_parameter_name": None}, id="null"),
        pytest.param({"credential_parameter_name": ""}, id="empty"),
        pytest.param({}, id="omitted"),
    ],
)
def test_readiness_uses_synthetic_credential_only_with_configured_parameter(
    credential_config: dict[str, str | None],
    monkeypatch: pytest.MonkeyPatch,
    offline_readiness_script: ModuleType,
) -> None:
    class AuthenticatedProvider(BaseProvider):
        @typing.override
        def readiness_credential(self) -> str | None:
            return json.dumps({"token": "EXAMPLE_READINESS_ONLY"})

        @typing.override
        def fetch(self, client: HttpClient, credential: str | None) -> object:
            if credential is None:
                raise ValueError("Provider credential is required")
            token = json.loads(credential)["token"]
            return client.get_json(
                url="https://example.com/current", headers={"Authorization": f"Bearer {token}"}
            )

        @typing.override
        def normalize(self, payload: object, observed_at: datetime) -> list[Observation]:
            raise AssertionError("Readiness must not normalize payloads")

    config_script = offline_readiness_script
    config = config_script.yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    config["provider"].pop("credential_parameter_name")
    config["provider"].update(
        name="readiness_test", base_url="https://example.com/current", **credential_config
    )
    provider = AuthenticatedProvider()
    synthetic_hook = create_autospec(
        provider.readiness_credential, side_effect=provider.readiness_credential
    )
    fetch = create_autospec(provider.fetch, side_effect=provider.fetch)
    monkeypatch.setattr(provider, "readiness_credential", synthetic_hook)
    monkeypatch.setattr(provider, "fetch", fetch)
    monkeypatch.setitem(config_script.PROVIDER_FACTORIES, "readiness_test", lambda *args: provider)
    monkeypatch.setattr(config_script.yaml, "safe_load", lambda _: config)

    if credential_config.get("credential_parameter_name"):
        assert config_script.readiness(CONFIG_PATH) is config
        synthetic_hook.assert_called_once_with()
        assert fetch.call_args.args[1] == json.dumps({"token": "EXAMPLE_READINESS_ONLY"})
        fetch.call_args.args[0].get_json.assert_called_once_with(
            url="https://example.com/current",
            headers={"Authorization": "Bearer EXAMPLE_READINESS_ONLY"},
        )
    else:
        with pytest.raises(ValueError, match="Provider credential is required"):
            config_script.readiness(CONFIG_PATH)
        synthetic_hook.assert_not_called()
        assert fetch.call_args.args[1] is None
        fetch.call_args.args[0].get_json.assert_not_called()
    fetch.assert_called_once()


def test_deployment_keeps_manual_changeset_confirmation(
    monkeypatch: pytest.MonkeyPatch,
    sam_parser: Callable[[list[str]], dict[str, str]],
) -> None:
    config_script = load_config_script()
    calls = []
    monkeypatch.setattr(config_script, "run", lambda command, **kwargs: calls.append(command))
    monkeypatch.setattr(config_script, "verify_package", lambda: calls.append(["verify_package"]))
    monkeypatch.setattr(sys, "argv", [str(SCRIPT_PATH), "deploy", str(CONFIG_PATH)])
    config_script.main()
    assert [command[:2] for command in calls] == [
        ["aws", "sts"],
        ["sam", "build"],
        ["find", ".aws-sam/build/FetcherFunction"],
        ["verify_package"],
        ["sam", "deploy"],
    ]
    assert calls[2] == [
        "find",
        ".aws-sam/build/FetcherFunction",
        "-type",
        "f",
        "-name",
        ".DS_Store",
        "-delete",
    ]
    deployment = calls[-1]
    assert "--confirm-changeset" in deployment
    assert "--no-confirm-changeset" not in deployment
    parsed = sam_parser(deployment[deployment.index("--parameter-overrides") + 1 :])
    assert parsed["CredentialParameterName"] == ""
    assert parsed["AlarmNotificationTopicArn"] == ""


@pytest.mark.parametrize(
    "state",
    ["valid", "stale", "extra", "dependency", "distribution", "build_failure", "cleanup_failure"],
)
def test_deployment_checks_final_build_before_deploy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, state: str
) -> None:
    config_script = load_config_script()
    config = config_script.readiness(CONFIG_PATH)
    monkeypatch.setattr(config_script, "readiness", lambda _: config)
    config_script.__file__ = str(tmp_path / "scripts" / "config.py")
    source = tmp_path / "src" / "scheduled_ingestor"
    built = tmp_path / ".aws-sam" / "build" / "FetcherFunction" / "scheduled_ingestor"
    for module in config_script.REQUIRED_MODULES:
        destination = source / module
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text("SNAPSHOT = True\n", encoding="utf-8")
    source.joinpath(".DS_Store").write_bytes(b"source metadata")
    unrelated = tmp_path / ".aws-sam" / "build" / "OtherFunction" / ".DS_Store"
    unrelated.parent.mkdir(parents=True)
    unrelated.write_bytes(b"unrelated metadata")
    calls = []
    verified = {}
    verify_package = config_script.verify_package

    def verify() -> None:
        calls.append(["verify_package"])
        verify_package()
        verified.update(
            {
                item.relative_to(built.parent): item.read_bytes()
                for item in built.parent.rglob("*")
                if item.is_file()
            }
        )

    def run(command: list[str], **kwargs: object) -> None:
        calls.append(command)
        if command == ["sam", "build"]:
            if state == "build_failure":
                raise RuntimeError("Synthetic build failure")
            shutil.copytree(source, built)
            built.parent.joinpath(".DS_Store").write_bytes(b"generated metadata")
            if state == "stale":
                built.joinpath("core", "models.py").write_text("STALE = True\n", encoding="utf-8")
            elif state == "extra":
                built.joinpath("extra.py").write_text("EXTRA = True\n", encoding="utf-8")
            elif state == "dependency":
                built.parent.joinpath("boto3.py").write_text("SDK = True\n", encoding="utf-8")
            elif state == "distribution":
                distribution = built.parent / "example.dist-info"
                distribution.mkdir()
                distribution.joinpath("METADATA").write_text("Name: example\n", encoding="utf-8")
        elif command[0] == "find":
            if state == "cleanup_failure":
                raise RuntimeError("Synthetic cleanup failure")
            subprocess.run(command, check=True)
        elif command[:2] == ["sam", "deploy"]:
            assert calls[-2] == ["verify_package"]
            assert verified == {
                item.relative_to(built.parent): item.read_bytes()
                for item in built.parent.rglob("*")
                if item.is_file()
            }

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(config_script, "run", run)
    monkeypatch.setattr(config_script, "verify_package", verify)
    monkeypatch.setattr(sys, "argv", [str(SCRIPT_PATH), "deploy", str(CONFIG_PATH)])

    if state == "valid":
        config_script.main()
        assert calls[-1][:2] == ["sam", "deploy"]
        assert not list(built.parent.rglob(".DS_Store"))
    else:
        with pytest.raises((ValueError, RuntimeError), match=r"package|Synthetic"):
            config_script.main()
        assert not any(command[:2] == ["sam", "deploy"] for command in calls)
        if state in {"build_failure", "cleanup_failure"}:
            assert ["verify_package"] not in calls
    assert source.joinpath(".DS_Store").read_bytes() == b"source metadata"
    assert unrelated.read_bytes() == b"unrelated metadata"


def test_missing_required_request_config_has_no_descriptor_fallback(tmp_path: Path) -> None:
    config_script = load_config_script()
    config = config_script.yaml.safe_load(CONFIG_PATH.read_text())
    del config["provider"]["base_url"]
    config_path = tmp_path / "config.yaml"
    config_path.write_text(json.dumps(config))
    with pytest.raises(ValueError, match="base_url"):
        config_script.load(config_path)


def test_stack_override_can_complete_explicitly_unconfigured_stack(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_script = load_config_script()
    config = config_script.yaml.safe_load(CONFIG_PATH.read_text())
    config["application"]["stack_name"] = None
    config_path = tmp_path / "config.yaml"
    config_path.write_text(json.dumps(config))
    monkeypatch.setenv("STACK_NAME", "example-override")
    loaded = config_script.load(config_path)
    assert loaded["storage"]["table_name"] == "example-override-observations"


def test_completed_fixture_deployment_runtime_and_snapshot_agree(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_script = load_config_script()
    config = config_script.readiness(CONFIG_PATH)
    for name, value in config_script.lambda_environment_values(config).items():
        monkeypatch.setenv(name, value)
    settings = Settings.from_environment()
    provider = create_provider(
        settings.provider_name, settings.api_base_url, settings.provider_options
    )
    payload = json.loads(
        CONFIG_PATH.with_name("open_meteo_response.json").read_text(), parse_float=Decimal
    )
    table = config_script.Mock()
    snapshots = provider.normalize(payload, datetime(2026, 1, 1, tzinfo=UTC))
    assert len(snapshots) == 1
    repository = ObservationRepository(table, settings.data_retention_days, settings.table_schema)
    assert repository.put(snapshots[0]) is True
    row = table.put_item.call_args.kwargs["Item"]
    assert set(row) == {"date", "ts", "expts", "temperature", "apparent"}
    assert row["date"] == Decimal(20261009)
    assert row["temperature"] == Decimal("20.25")
    assert row["apparent"] == Decimal("19.5")


@pytest.mark.parametrize(
    "state",
    [
        "valid",
        "missing",
        "descriptor",
        "stale",
        "extra",
        "dependency",
        "distribution",
        "nested_dependency",
    ],
)
def test_package_gate_rejects_missing_stale_or_descriptor_payload(
    tmp_path: Path,
    state: str,
) -> None:
    config_script = load_config_script()
    config_script.__file__ = str(tmp_path / "scripts" / "config.py")
    source = tmp_path / "src" / "scheduled_ingestor"
    built = tmp_path / ".aws-sam" / "build" / "FetcherFunction" / "scheduled_ingestor"
    for directory in (source, built):
        for module in config_script.REQUIRED_MODULES:
            destination = directory.joinpath(module)
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text("SNAPSHOT = True\n")
    if state == "missing":
        built.joinpath("core", "models.py").unlink()
    elif state == "descriptor":
        built.joinpath("providers", "open_meteo.yaml").write_text("provider: {}\n")
    elif state == "stale":
        built.joinpath("core", "models.py").write_text("SNAPSHOT = False\n")
    elif state == "extra":
        built.joinpath("obsolete.py").write_text("OBSOLETE = True\n")
    elif state == "dependency":
        built.parent.joinpath("boto3.py").write_text("SDK = True\n")
    elif state == "distribution":
        distribution = built.parent / "example-1.0.dist-info"
        distribution.mkdir()
        distribution.joinpath("METADATA").write_text("Name: example\n")
    elif state == "nested_dependency":
        built.joinpath("dependency.so").write_bytes(b"synthetic")
    if state == "valid":
        config_script.verify_package()
    else:
        with pytest.raises(ValueError, match=r"package|descriptor"):
            config_script.verify_package()


@pytest.mark.parametrize("location", ["source", "built"])
@pytest.mark.parametrize(
    "obsolete",
    [
        "config.py",
        "http.py",
        "models.py",
        "repository.py",
        "storage.py",
        "providers/base.py",
        "providers/registry.py",
        "providers/open_meteo.py",
    ],
)
def test_package_gate_rejects_obsolete_module_even_when_source_and_build_match(
    tmp_path: Path, location: str, obsolete: str
) -> None:
    config_script = load_config_script()
    config_script.__file__ = str(tmp_path / "scripts" / "config.py")
    source = tmp_path / "src" / "scheduled_ingestor"
    built = tmp_path / ".aws-sam" / "build" / "FetcherFunction" / "scheduled_ingestor"
    for directory in (source, built):
        for module in config_script.REQUIRED_MODULES:
            destination = directory.joinpath(module)
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text("SNAPSHOT = True\n")
    destination = source if location == "source" else built
    destination.joinpath(obsolete).write_text("LEGACY = True\n")

    with pytest.raises(ValueError, match="obsolete modules"):
        config_script.verify_package()


def test_readiness_rejects_structural_provider_without_explicit_abc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_script = load_config_script()
    monkeypatch.setattr(config_script, "create_provider", lambda *args: config_script.Mock())

    with pytest.raises(ValueError, match="must inherit BaseProvider"):
        config_script.readiness(CONFIG_PATH)
