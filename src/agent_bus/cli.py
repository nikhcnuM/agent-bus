from __future__ import annotations

import argparse

import uvicorn

from agent_bus.app import create_app
from agent_bus.config import load_config


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local Agent Bus.")
    parser.add_argument("--config", help="Path to YAML config file.")
    args = parser.parse_args()

    config = load_config(args.config)
    uvicorn.run(create_app(config), host=config.server.host, port=config.server.port)
