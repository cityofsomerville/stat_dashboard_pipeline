import yaml
import logging

from stat_dashboard_pipeline.definitions import AUTH_PATH, CATEGORY_PATH, PERMIT_CATEGORY_PATH

# TODO: DRY this up
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
                logging.error(exc)
                return None
        return creds


class Config():
    """
    Get variable configs from 'config' dir
    """

    def __init__(self):
        self.qscend_category_file = CATEGORY_PATH
        self.permit_category_file = PERMIT_CATEGORY_PATH

    def qscend_categories(self):
        with open(self.qscend_category_file, 'r') as stream:
            try:
                data = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                logging.error(exc)
                return None
        return data

    def permit_categories(self):
        with open(self.permit_category_file, 'r') as stream:
            try:
                data = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                logging.error(exc)
                return None
        return data
