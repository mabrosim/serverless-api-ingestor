import json
import os
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest
import yaml


@pytest.fixture
def isolated_process(request: pytest.FixtureRequest) -> Callable[[Callable[[], None]], None]:
    """Rerun this test node in a fresh interpreter, with bounded kill/reap on failure."""

    def run(check: Callable[[], None]) -> None:
        if os.environ.get("INGESTOR_TIMER_TEST_CHILD") == "1":
            check()
            return
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "-o", "addopts=-q", "--no-cov", request.node.nodeid],
            env={**os.environ, "INGESTOR_TIMER_TEST_CHILD": "1"},
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=3.0,
            check=False,
        )
        assert result.returncode == 0, result.stdout

    return run


@pytest.fixture(autouse=True)
def runtime_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Supply explicit sanitized runtime schema/options; never production settings."""
    config_path = Path(__file__).with_name("fixtures") / "config.yaml"
    config = yaml.safe_load(config_path.read_text())
    storage = config["storage"]
    options = config["provider"]["options"]
    schema = {
        "partition_key": storage["partition_key"],
        "sort_key": storage["sort_key"],
        "ttl": storage["ttl"],
        "fields": [
            storage["partition_key"],
            storage["sort_key"],
            {"name": storage["ttl"]["attribute_name"], "type": "N"},
            *[{"name": item["name"], "type": item["type"]} for item in options["measurements"]],
        ],
    }
    monkeypatch.setenv("STORAGE_SCHEMA_JSON", json.dumps(schema))
    monkeypatch.setenv("PROVIDER_OPTIONS_JSON", json.dumps(options))
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
