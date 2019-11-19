import yaml

from stat_dashboard_pipeline.definitions import AUTH_PATH

class Auth():
    """
    dump credentials into mem
    """

    def __init__(self):
        self.auth_file = AUTH_PATH

    def credentials(self):
        with open(self.auth_file, 'r') as stream:
            try:
                creds = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
                return None
        return creds
