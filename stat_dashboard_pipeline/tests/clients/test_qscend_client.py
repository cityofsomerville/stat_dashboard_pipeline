import os
import unittest

from freezegun import freeze_time

from stat_dashboard_pipeline.clients.qscend_client import QScendClient

class QScendClientTest(unittest.TestCase):

    def setUp(self):
        self.qscend_client = QScendClient()
        self.qscend_client.credentials = {
            'qscend_key': '',
        }

    @freeze_time("2012-01-14")
    def test_format_date(self):
        formatted_date = self.qscend_client.format_date(time_window=4)
        self.assertEqual('01/10/2012', formatted_date)

    def test_get_departments(self):
        url = os.path.join('http://example.com', 'foo', 'bar')
        querystring = {}
        departments = self.qscend_client.generate_response(url, querystring)
        print(departments)
        self.assertEqual(None, departments)
