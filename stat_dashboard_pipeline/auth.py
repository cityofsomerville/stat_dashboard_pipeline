import yaml

from stat_dashboard_pipeline.definitions import CONFIG_PATH

class Auth():
    """
    dump credentials into mem
    """

    def __init__(self):
        self.config_file = CONFIG_PATH

    def credentials(self):
        with open(self.config_file, 'r') as stream:
            try:
                creds = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
                return None
        return creds
