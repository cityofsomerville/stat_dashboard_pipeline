import os
import unittest
import datetime
import csv

from freezegun import freeze_time
import ddt

from stat_dashboard_pipeline.pipeline.citizenserve import CitizenServePipeline
from stat_dashboard_pipeline.config import ROOT_DIR

@ddt.ddt
class CitizenServePipelineTest(unittest.TestCase):

    def setUp(self):
        self.citizenserve = CitizenServePipeline()
        self.citizenserve.credentials = {
            'sftp_remote_dir': '',
            'sftp_server': '',
            'sftp_port': 0
        }
        self.temp_file = os.path.join(ROOT_DIR, 'text.csv')

    def tearDown(self):
        try:
            os.remove(self.temp_file)
        except FileNotFoundError:
            pass

    def _generate_test_file(self, test_data):
        fieldnames = []
        for key in test_data.keys():
            fieldnames.append(key)
        with open(self.temp_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(
                csvfile,
                delimiter='\t',
                fieldnames=fieldnames
            )
            writer.writeheader()
            writer.writerow(test_data)

    @ddt.data([{
        'Permit#': '150',
        'PermitType': 'Construction',
        'IssueDate': '11/07/2014',
        'ApplicationDate': '11/07/2014',
        'Status': "Issued",
        'PermitAmount': 278.00,
        'Latitude': 0,
        'Longitude': 0,
        'Address': '93  HIGHLAND   ave',
        'ProjectName': 'REPLACE   SOMERSTAT DaTa DASHBOARD'
    }])
    @ddt.unpack
    def test_groom_permits(self, permit_data):
        self._generate_test_file(test_data=permit_data)
        self.citizenserve.groom_permits(temp_file=self.temp_file)
        try:
            permit_id = permit_data['Permit#']
            permit_dict = self.citizenserve.permits[permit_id]
        except KeyError:
            self.assertEqual(5, 6)
        issue_date = datetime.datetime.strptime(
            permit_data['IssueDate'],
            '%m/%d/%Y'
        )
        application_date = datetime.datetime.strptime(
            permit_data['ApplicationDate'],
            '%m/%d/%Y'
        )

        self.assertEqual(
            permit_dict['type'],
            permit_data['PermitType']
        )
        self.assertEqual(
            permit_dict['status'],
            permit_data['Status']
        )
        self.assertEqual(
            permit_dict['amount'],
            str(permit_data['PermitAmount'])
        )
        self.assertEqual(
            permit_dict['latitude'],
            str(permit_data['Latitude'])
        )
        self.assertEqual(
            permit_dict['longitude'],
            str(permit_data['Longitude'])
        )
        self.assertEqual(
            permit_dict['issue_date'],
            issue_date
        )
        self.assertEqual(
            permit_dict['application_date'],
            application_date
        )
        # Tests grooming as well
        self.assertEqual(
            permit_dict['work'],
            'Replace somerstat data dashboard'
        )
        self.assertEqual(
            permit_dict['address'],
            '93 Highland Ave'
        )



    @ddt.data([
        {
            "Commercial": "Commercial Building",
            "Commercial - Existing": "Commercial Building",
        },
        ['Commercial', 'Residential', 'Random Category']
    ])
    @ddt.unpack
    def test_determine_categories(self, category_data, test_cats):
        self.citizenserve.categories = category_data
        for category in test_cats:
            return_cat = self.citizenserve.determine_categories(
                permit_type=category
            )
            if category in category_data.keys():
                self.assertEqual(
                    category_data[category],
                    return_cat
                )
            else:
                self.assertEqual(
                    category,
                    return_cat
                )

    @freeze_time("2012-01-14")
    def test_format_dates(self):
        formatted_date = self.citizenserve.format_dates(
            "1/14/2012"
        )
        self.assertEqual(
            datetime.datetime.now(),
            formatted_date
        )
