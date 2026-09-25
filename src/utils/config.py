from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"


def load_config():
    """Load the project configuration from YAML."""
    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def get_path(config, key):
    """Return an absolute project path from the configuration."""
    return PROJECT_ROOT / config["paths"][key]