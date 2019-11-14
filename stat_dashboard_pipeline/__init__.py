"""
SomerStat Daily Data Dashboard
Pipeline
"""
NAME = "stat_dashboard_pipeline"

from stat_dashboard_pipeline.clients.sftp_client import SFTPClient
from stat_dashboard_pipeline.pipeline.qscend import QScendPipeline

class Pipeline():
    """
    Parent pipeline class, with methods favorable to the enduser CLI

    """
    def __init__(self):
        self.placeholder_string = 'Hello Somerville'
        self.citizenserve = SFTPClient()
        self.qscend = QScendPipeline()

    def run(self):
        """
        Nominal running of pipeline code
        """
        print(self.placeholder_string)
        self.qscend.run()

    def collect(self):
        return

    def groom(self):
        return
