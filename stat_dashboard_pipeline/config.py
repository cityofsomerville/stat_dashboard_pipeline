import os
import logging

import yaml

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
QSCEND_CATEGORY_FILE = 'qscend_categories.json'
PERMIT_CATEGORY_FILE = 'permit_categories.json'
SOCRATA_CONFIG = 'socrata.json'
QSCEND_CONFIG = 'qscend_status_codes.json'


class Config():
    """
    Get variable configs from 'config' dir, including auth
    """
    def __init__(self):
        self.qscend_categories = self.__parse_json(os.path.join(
            ROOT_DIR,
            'config',
            QSCEND_CATEGORY_FILE
        ))
        self.permit_categories = self.__parse_json(os.path.join(
            ROOT_DIR,
            'config',
            PERMIT_CATEGORY_FILE
        ))
        self.credentials = self.__parse_json(os.path.join(
            ROOT_DIR,
            'config',
            'auth.yaml'
        ))
        self.socrata_datasets = self.__parse_json(os.path.join(
            ROOT_DIR,
            'config',
            SOCRATA_CONFIG
        ))
        self.qscend_statuses = self.__parse_json(os.path.join(
            ROOT_DIR,
            'config',
            QSCEND_CONFIG
        ))

    @staticmethod
    def __parse_json(file):
        if not (os.path.exists(file)):
            logging.error('[CONFIG] Missing file: %s', file)
            return {}
        with open(file, 'r') as stream:
            try:
                data = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                logging.error(exc)
                return None
        return data
