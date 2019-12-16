"""
constants
"""
import os

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AUTH_PATH = os.path.join(ROOT_DIR, 'config', 'auth.yaml')
CATEGORY_PATH = os.path.join(ROOT_DIR, 'config', 'qscend_categories.json')
PERMIT_CATEGORY_PATH = os.path.join(ROOT_DIR, 'config', 'permit_categories.json')
