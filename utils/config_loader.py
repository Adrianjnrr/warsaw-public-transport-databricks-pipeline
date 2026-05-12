import yaml


def load_config(path='config/datasets.yml'):
    with open(path, "r") as r:
        return yaml.safe_load(r)
