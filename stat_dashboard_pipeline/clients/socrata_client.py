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


class SocrataClient():

    def __init__(self, **kwargs):
        self._credentials = self.__load_credentials()
        self.client = None
        self.service_data = kwargs.get('service_data', None)
        self.dataset_id = kwargs.get('dataset_id', None)
        self.citizenserve_update_window = kwargs.get('citizenserve_update_window', 90)

    def run(self):
        self.upsert_citizenserve()

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

    def upsert_citizenserve(self):
        """
        NOTE: Unique ID in Socrata is set via UI
        https://support.socrata.com/hc/en-us/articles/ \
            360008065493-Setting-a-Row-Identifier-in-the-Socrata-Data-Management-Experience
        """
        if self.dataset_id is None:
            print('[SOCRATA_CLIENT] No Socrata dataset ID provided')
            return
        if self.client is None:
            self._connect()
        groomed_data = self.dict_transform()
        data = []
        for row in groomed_data[0]:
            # TODO: Make generic, DRY up
            if row['application_date'] > \
                datetime.datetime.now() - timedelta(days=self.citizenserve_update_window):
                for key, entry in row.items():
                    # Deformat dates
                    if isinstance(entry, datetime.datetime):
                        replacement = self.__deformat_date(entry)
                        row[key] = replacement
                data.append(row)

        print('[SOCRATA_CLIENT] Upserting data')
        self.client.upsert(self.dataset_id, data)

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
        # NOTE: This isn't working, (HTTPS timeout)
        # but can be/was uploaded using UI
        self.client.replace(T11_DATASET_ID, data)

    def dict_transform(self):
        fieldnames = set()
        final_report = []
        for key, entry in self.service_data.items():
            fieldnames.add('id')
            # Add ID, go into dict
            row = {'id': key}
            for k, e in entry.items():
                row[k] = e
                fieldnames.add(k)
            final_report.append(row)
        return (final_report, fieldnames)

    def __json_to_csv(self):
        """
        Convert JSON to temporary CSV file
        for initial upload
        TODO: Move to pipeline and break into methods
        """
        tempfile = os.path.join(
            ROOT_DIR,
            'tmp',
            'soctemp.csv'
        )
        fieldnames = set()
        final_report = []
        for key, entry in self.service_data.items():
            fieldnames.add('id')
            # Add ID, go into dict
            row = {'id': key}
            for k, e in entry.items():
                row[k] = e
                fieldnames.add(k)
            final_report.append(row)

        with open(tempfile, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=list(fieldnames))
            writer.writeheader()
            for row in final_report:
                writer.writerow(row)
        return

    @staticmethod
    def __deformat_date(date):
        return date.isoformat()
