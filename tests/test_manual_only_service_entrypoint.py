from pathlib import Path

import pytest

from app.config import ManualOnlyConfigurationError, load_config, require_manual_only


ROOT = Path(__file__).parents[1]
UNIT = ROOT / "deploy" / "short-telegram-bot-lite.service.example"


def test_service_unit_pins_working_directory_interpreter_and_live_script() -> None:
    text = UNIT.read_text(encoding="utf-8")
    lines = dict(
        line.split("=", 1)
        for line in text.splitlines()
        if "=" in line and not line.lstrip().startswith("#")
    )

    assert "<APP_ROOT>" not in text
    assert "current" not in text
    assert lines["WorkingDirectory"] == "<RELEASE_DIR>"
    assert lines["ExecStart"] == (
        "<RELEASE_DIR>/.venv/bin/python <RELEASE_DIR>/scripts/run_live.py"
    )
    assert text.count("scripts/run_live.py") == 1
    assert "Environment=PYTHONUNBUFFERED=1" in text
    assert "Environment=AUTOEXECUTION=OFF" in text
    assert lines["User"] == "root"
    assert lines["Restart"] == "always"


def test_manual_only_runtime_requires_explicit_off(tmp_path: Path) -> None:
    config = tmp_path / "config.yaml"
    config.write_text("shortlist_size: 100\n", encoding="utf-8")

    with pytest.raises(ManualOnlyConfigurationError, match="AUTOEXECUTION"):
        require_manual_only(load_config(config_path=config, env_path=tmp_path / ".env"))


def test_manual_only_runtime_rejects_non_off(tmp_path: Path) -> None:
    config = tmp_path / "config.yaml"
    config.write_text("AUTOEXECUTION: ON\n", encoding="utf-8")

    with pytest.raises(ManualOnlyConfigurationError, match="OFF"):
        require_manual_only(load_config(config_path=config, env_path=tmp_path / ".env"))


def test_manual_only_runtime_accepts_explicit_off(tmp_path: Path) -> None:
    config = tmp_path / "config.yaml"
    config.write_text("AUTOEXECUTION: OFF\n", encoding="utf-8")

    assert require_manual_only(
        load_config(config_path=config, env_path=tmp_path / ".env")
    ).autoexecution == "OFF"


def test_manual_only_runtime_reads_systemd_environment_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = tmp_path / "config.yaml"
    config.write_text("shortlist_size: 100\n", encoding="utf-8")
    monkeypatch.setenv("AUTOEXECUTION", "OFF")

    assert require_manual_only(
        load_config(config_path=config, env_path=tmp_path / ".env")
    ).autoexecution == "OFF"
