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


class CitizenServePipeline():

    def __init__(self):
        self.cs_client = CitizenServeClient()
        self.permits = {}
        self.categories = self.get_categories()

    def run(self):
        """
        Semi-temp master run funct
        """
        temp_file = self.get_data()
        if temp_file is not None:
            self.groom_data(temp_file)

    def groom_data(self, temp_file):
        """
        The SFTP dump appears to be 'everything since 2015'
        So we'll overwrite and create a fresh JSON for upload
        """
        with open(temp_file, 'r', encoding="ISO-8859-1") as data:
            datareader = csv.DictReader(data, delimiter='\t')
            for row in datareader:
                permit_id = row['Permit#']
                permit_type = self.determine_categories(row['PermitType'])
                self.permits[permit_id] = {
                    'type': permit_type,
                    'issue_date': self.__format_dates(row['IssueDate']),
                    'application_date': self.__format_dates(row['ApplicationDate']),
                    'status': row['Status'],
                    'amount': row['PermitAmount'],
                    # TODO: Anonymize?
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
            self.cs_client.download()
        except paramiko.ssh_exception.AuthenticationException:
            logging.error('Credentials failure, Citizenserve SFTP')
            return None
        return self.cs_client.local_path()

    @staticmethod
    def __format_dates(date):
        return datetime.datetime.strptime(date, '%m/%d/%Y')

    @staticmethod
    def get_categories():
        """
        These are inhereted from the prior repo, and can
        be updated in 'config/qscend_cat_id_key.json'
        """
        config = Config()
        return config.permit_categories
