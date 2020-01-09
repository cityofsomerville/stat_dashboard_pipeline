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
import logging

from sodapy import Socrata

from stat_dashboard_pipeline.config import Config, ROOT_DIR

SOCRATA_MASTER_TIMEOUT = 600

class SocrataClient(Config):

    def __init__(self, **kwargs):
        self.client = None
        self.service_data = kwargs.get('service_data', None)
        self.dataset_id = kwargs.get('dataset_id', None)
        super(SocrataClient, self).__init__()

    def _connect(self):
        self.client = Socrata(
            self.credentials['socrata_url'],
            self.credentials['socrata_token'],
            self.credentials['socrata_username'],
            self.credentials['socrata_password']
        )
        # See:
        # https://stackoverflow.com/questions/47514331/readtimeout-error-for-api-data-with-sodapy-client
        self.client.timeout = SOCRATA_MASTER_TIMEOUT

    def upsert(self):
        """
        NOTE: Unique ID in Socrata is set via UI
        https://support.socrata.com/hc/en-us/articles/ \
            360008065493-Setting-a-Row-Identifier-in-the-Socrata-Data-Management-Experience
        """
        if self.dataset_id is None:
            logging.error('[SOCRATA_CLIENT] No Socrata dataset ID provided')
            return
        if self.client is None:
            self._connect()
        groomed_data = self.dict_transform()
        data = self.send_data(
            groomed_data=groomed_data
        )

        logging.info('[SOCRATA_CLIENT] Upserting data')
        logging.info(self.client.upsert(self.dataset_id, data))
    
    def send_data(self, groomed_data):
        data = []
        for row in groomed_data:
            for key, entry in row.items():
                # Deformat and replace dates
                if isinstance(entry, datetime.datetime):
                    replacement = self.__deformat_date(entry)
                    row[key] = replacement
            data.append(row)
        return data

    def dict_transform(self):
        final_report = []
        for key, entry in self.service_data.items():
            # Add ID, go into dict
            row = {'id': key}
            for subkey, subent in entry.items():
                row[subkey] = subent
            final_report.append(row)
        return final_report

    def json_to_csv(self, filename='soctemp.csv'):
        """
        Convert JSON to temporary CSV file
        for initial upload
        """
        tempfile = os.path.join(
            ROOT_DIR,
            'tmp',
            filename
        )
        fieldnames = set()
        final_report = []
        for key, entry in self.service_data.items():
            fieldnames.add('id')
            # Add ID, go into dict
            row = {'id': key}
            for subkey, subent in entry.items():
                row[subkey] = subent
                fieldnames.add(subkey)
            final_report.append(row)

        with open(tempfile, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=list(fieldnames))
            writer.writeheader()
            for row in final_report:
                writer.writerow(row)

    @staticmethod
    def __deformat_date(date):
        return date.isoformat()
