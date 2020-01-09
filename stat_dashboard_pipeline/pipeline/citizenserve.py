"""
Grooming for Citizenserve SFTP return

Raw CSV SFTP Dumps -> Socrata Storable JSON
"""
import csv
import datetime
import logging

import paramiko

from stat_dashboard_pipeline.clients.citizenserve_client import CitizenServeClient
from stat_dashboard_pipeline.config import Config


class CitizenServePipeline(CitizenServeClient):

    def __init__(self):
        self.permits = {}
        self.categories = self.get_categories()
        super().__init__()

    def groom_permits(self):
        """
        The SFTP dump appears to be 'everything since 2015'
        So we'll overwrite and create a fresh JSON for upload
        """
        temp_file = self.get_data()
        if not temp_file:
            return

        with open(temp_file, 'r', encoding="ISO-8859-1") as data:
            datareader = csv.DictReader(data, delimiter='\t')
            for row in datareader:
                permit_id = row['Permit#']
                permit_type = self.determine_categories(row['PermitType'])
                self.permits[permit_id] = {
                    'type': permit_type,
                    'issue_date': self.format_dates(row['IssueDate']),
                    'application_date': self.format_dates(row['ApplicationDate']),
                    'status': row['Status'],
                    'amount': row['PermitAmount'],
                    'latitude': row['Latitude'],
                    'longitude': row['Longitude']
                }

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
