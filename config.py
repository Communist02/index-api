import json
import os
from typing import Any

config_path = os.path.expanduser('~/index-api.json')

required_fields = [
    'debug_mode'
    's3_url',
    'opensearch_host',
    'opensearch_port',
    'opensearch_user',
    'opensearch_password',
    'opensearch_files_index',
    's3_access_key',
    's3_secret_key'
]

default_config = {
    's3_url': "localhost:9000",
    'debug_mode': True,
    'opensearch_host': "elastic-1.eco.dvo.ru",
    'opensearch_port': 9200,
    'opensearch_user': "admin",
    'opensearch_password': "OTFiZDkwMGRiOWQw1!",
    'opensearch_files_index': "collections-files",
    's3_access_key': "admin",
    's3_secret_key': "password"
}


class Config:
    def __init__(self, config_path=config_path):
        try:
            with open(config_path, 'r') as file:
                self.config = json.load(file)
                print(f"Config loaded from: {config_path}")
                self._validate_required_fields()
        except FileNotFoundError:
            print(f"Config file not found at {config_path}, created new file")
            with open(config_path, 'w') as file:
                file.write(json.dumps(default_config, indent=4))

            self.config = default_config
        except json.JSONDecodeError as e:
            print(f"Invalid JSON in config file at {config_path}: {e}")
            self.config = default_config

    def _validate_required_fields(self):
        """Проверяет наличие всех обязательных полей"""
        missing_fields = []

        for field in required_fields:
            if field not in self.config or self.config.get(field) is None:
                missing_fields.append(field)

        if missing_fields:
            print(f"Missing required fields from config: {', '.join(missing_fields)}")

    def __getattr__(self, name: str) -> Any:
        return self.config.get(name)

    def get(self, key: str, default: Any = None) -> Any:
        return self.config.get(key, default)


config = Config()
