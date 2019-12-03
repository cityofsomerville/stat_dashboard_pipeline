"""
Socrata Data Store Client

NOTE: Socrata only takes Comma-separated values (CSV),
Tab-separated values (TSV), Microsoft Excel (XLS),
Microsoft Excel (OpenXML), ZIP archive (shapefile),
JSON format (GeoJSON), GeoJSON format,
Keyhole Markup Language (KML), Zipped Keyhole Markup Language (KMZ)

So: Convert the stored JSON to CSV for initial imports
HOWEVER: The API will 'upsert' a row if the ID (set in the UI) is identifiable

"""
import os
import datetime
from datetime import timedelta
import csv

from sodapy import Socrata

from stat_dashboard_pipeline.config import Auth
from stat_dashboard_pipeline.definitions import ROOT_DIR

# TODO: Move to config
T11_DATASET_ID = '8bfi-2uyk'

class SocrataClient():

    def __init__(self, **kwargs):
        self._credentials = self.__load_credentials()
        self.client = None
        self.service_data = kwargs.get('service_data', None)
        self.store_data = []

    def run(self):
        self.__json_to_csv()
        self.upsert_data_records()

    @staticmethod
    def __load_credentials():
        # TODO: build into Auth methods
        auth = Auth()
        return auth.credentials()

    def _connect(self):
        self.client = Socrata(
            self._credentials['socrata_url'],
            self._credentials['socrata_token'],
            self._credentials['socrata_username'],
            self._credentials['socrata_password']
        )

    def upsert_data_records(self):
        """
        NOTE: Unique ID in Socrata is set via UI
        https://support.socrata.com/hc/en-us/articles/360008065493-Setting-a-Row-Identifier-in-the-Socrata-Data-Management-Experience
        """
        if self.client is None:
            self._connect()
        data = []
        for key, entry in self.service_data.items():
            data.append({
                'id': key,
                'this_is_a_test': entry ['this_is_a_test'],
                'this_is_also_a_test': entry['this_is_a_test'],
            })
        self.client.upsert(T11_DATASET_ID, data)

    def replace_data_json(self):
        """
        Full dataset replacement
        """
        if self.client is None:
            self._connect()
        tempfile = os.path.join(
            ROOT_DIR,
            'tmp',
            'soctemp.csv'
        )
        data = open(tempfile)
        self.client.replace(T11_DATASET_ID, data)


    def __json_to_csv(self):
        """
        Convert JSON to temporary CSV file for replacement/initial upload
        """
        tempfile = os.path.join(
            ROOT_DIR,
            'tmp',
            'soctemp.csv'
        )
        with open(tempfile, 'w', newline='') as csvfile:
            fieldnames = ['id', 'this_is_a_test', 'this_is_also_a_test']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for key, entry in self.service_data.items():
                # for k, e in entry.items():
                writer.writerow({
                    'id': key, 
                    'this_is_a_test': entry['this_is_a_test'], 
                    'this_is_also_a_test': entry['this_is_also_a_test']
                })
            return


if __name__ == '__main__':
    scstore = SocrataClient(
        service_data={1: {
            "this_is_a_test": "test3",
            "this_is_also_a_test": "test3"
        }}
    )
    scstore.run()

