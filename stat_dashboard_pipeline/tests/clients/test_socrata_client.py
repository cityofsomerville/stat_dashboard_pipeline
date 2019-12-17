import os
import unittest

from stat_dashboard_pipeline.clients.socrata_client import SocrataClient
from stat_dashboard_pipeline.config import ROOT_DIR


class SocrataClientTest(unittest.TestCase):

    def setUp(self):
        self.socrata_client = SocrataClient()
        self.socrata_client.service_data = {
            1: {
                'test_1': 'one_one',
                'test_2': 'one_two',
                'test_3': 'one_three'
            },
            2: {
                'test_1': 'two_one',
                'test_2': 'two_two',
                'test_3': 'two_three'
            },
            3: {
                'test_1': 'three_one',
                'test_2': 'three_two',
                'test_3': 'three_three'
            }
        }

    def tearDown(self):
        try:
            os.remove(os.path.join(
                ROOT_DIR,
                'tmp',
                'test.csv'
            ))
        except FileNotFoundError:
            pass

    def test_dict_transform(self):
        output = [
            {
                'id': 1,
                'test_1': 'one_one',
                'test_2': 'one_two',
                'test_3': 'one_three'
            },
            {
                'id': 2,
                'test_1': 'two_one',
                'test_2': 'two_two',
                'test_3': 'two_three'
            },
            {
                'id': 3,
                'test_1': 'three_one',
                'test_2': 'three_two',
                'test_3': 'three_three'
            }
        ]
        fields_output = {'id', 'test_2', 'test_3', 'test_1'}
        final_report, fieldnames = self.socrata_client.dict_transform()
        self.assertEqual(final_report, output)
        self.assertEqual(fieldnames, fields_output)

    def test_json_to_csv(self):
        self.socrata_client.json_to_csv(filename='test.csv')
        self.assertTrue(os.path.exists(os.path.join(
            ROOT_DIR,
            'tmp',
            'test.csv'
        )))
