import os
import unittest

from freezegun import freeze_time

from stat_dashboard_pipeline.clients.citizenserve_client import CitizenServeClient
from stat_dashboard_pipeline.config import ROOT_DIR

class CitizenServeClientTest(unittest.TestCase):

    def setUp(self):
        self.citizenserve_client = CitizenServeClient()

    @freeze_time("2012-01-14")
    def test_generate_filename(self):
        filename = self.citizenserve_client.generate_filename(days_prior=4)
        self.assertEqual('PermitExport01102012.txt', filename)

    def test_local_path(self):
        self.citizenserve_client.filename = 'test.txt'
        localpath = self.citizenserve_client.local_path()
        self.assertEqual(os.path.join(ROOT_DIR, 'tmp', 'test.txt'), localpath)
