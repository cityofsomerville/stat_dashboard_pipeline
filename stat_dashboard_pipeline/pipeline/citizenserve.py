"""
Grooming for Citizenserve SFTP return

Raw CSV SFTP Dumps -> Socrata Storable JSON
"""
import csv
import datetime
from datetime import timedelta
import logging

import paramiko

from stat_dashboard_pipeline.clients.citizenserve_client import CitizenServeClient
from stat_dashboard_pipeline.config import Config


class CitizenServePipeline(CitizenServeClient):

    def __init__(self, **kwargs):
        self.permits = {}
        self.categories = self.get_categories()
        self.update_window = kwargs.get('update_window', None)
        super().__init__()

    def groom_permits(self, temp_file=None):
        """
        The SFTP dump appears to be 'everything since 2015'
        So we'll overwrite and create a fresh JSON for upload
        """
        if not temp_file:
            temp_file = self.get_data()
            if not temp_file:
                return

        with open(temp_file, 'r', encoding="ISO-8859-1") as data:
            datareader = csv.DictReader(data, delimiter='\t')
            for row in datareader:
                permit_id = row['Permit#']
                permit_type = self.determine_categories(row['PermitType'])
                # Determine Data to Groom
                if self.determine_update_window(date=row['IssueDate']):
                    self.permits[permit_id] = {
                        'type': permit_type,
                        'issue_date': self.format_dates(row['IssueDate']),
                        'application_date': self.format_dates(row['ApplicationDate']),
                        'status': row['Status'],
                        'amount': row['PermitAmount'],
                        'latitude': row['Latitude'],
                        'longitude': row['Longitude'],
                        'address': self.groom_address(row['Address']),
                        'work': self.groom_work_field(row['ProjectName'])
                    }

    def determine_update_window(self, date):
        if not self.update_window:
            return True
        if self.format_dates(date) > datetime.datetime.now() - timedelta(days=self.update_window):
            return True
        return False

    def determine_categories(self, permit_type):
        try:
            self.categories[permit_type]
        except KeyError:
            return permit_type
        else:
            return self.categories[permit_type]

    def get_data(self):
        try:
            super().download()
        except paramiko.ssh_exception.AuthenticationException:
            logging.error('Credentials failure, Citizenserve SFTP')
            self.connection.close()
            return None
        except paramiko.ssh_exception.SSHException:
            logging.error('Credentials failure, Citizenserve SFTP')
            self.connection.close()
            return None
        return super().local_path()

    @staticmethod
    def format_dates(date):
        return datetime.datetime.strptime(date, '%m/%d/%Y')

    @staticmethod
    def get_categories():
        """
        These are inhereted from the prior repo, and can
        be updated in 'config/qscend_cat_id_key.json'
        """
        config = Config()
        return config.permit_categories

    @staticmethod
    def groom_work_field(raw_work):
        return ' '.join(
            raw_work.split()
        ).replace(',', '').capitalize().strip()

    @staticmethod
    def groom_address(raw_addy):
        return ' '.join(
            raw_addy.split()
        ).strip().title()
