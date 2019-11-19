"""
Grooming for Citizenserve SFTP return

Raw CSV SFTP Dumps -> Socrata Storable JSON
"""
import csv

import paramiko

from stat_dashboard_pipeline.clients.citizenserve_client import CitizenServeClient

class CitizenServePipeline():

    def __init__(self):
        self.cs_client = CitizenServeClient()
        self.permits = {}
        self.types = set()

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
                self.permits[permit_id] = {
                    'type': row['PermitType'],
                    'issue_date': row['IssueDate'],
                    'application_date': row['ApplicationDate'],
                    'status': row['Status'],
                    'amount': row['PermitAmount'],
                    'latitude': row['Latitude'],
                    'longitude': row['Longitude']
                }
                self.types.add(row['PermitType'])

    def get_data(self):
        try:
            self.cs_client.download()
        except paramiko.ssh_exception.AuthenticationException:
            return None
        return self.cs_client.local_path()
