"""
SomerStat Daily Data Dashboard
Pipeline
"""
import os
import shutil

from stat_dashboard_pipeline.pipeline.citizenserve import CitizenServePipeline
from stat_dashboard_pipeline.pipeline.qscend import QScendPipeline
from stat_dashboard_pipeline.clients.socrata_client import SocrataClient
from stat_dashboard_pipeline.definitions import ROOT_DIR


NAME = "stat_dashboard_pipeline"
# TODO: Move to config
T11_DATASET_ID = '8bfi-2uyk'
CS_DATASET_ID = 'ec6w-s4am'
CITIZENSERVE_UPDATE_WINDOW = 90

class Pipeline():
    """
    Parent pipeline class, with methods favorable to the enduser CLI

    """
    def __init__(self):
        self.citizenserve = CitizenServePipeline()
        self.qscend = QScendPipeline()
        self.socrata = None

    def run(self):
        """
        Nominal running of pipeline code
        Products:
            self.qscend.requests()
            self.citizenserve.permits
            self.citizenserve.types
        """
        # Create temp storage, if not there
        self.__prepare()
        # TODO: convert to returns, store client next
        # self.qscend.run()
        self.citizenserve.run()
        # TODO: clean up CS types
        self.store_citizenserve()
        # Cleanup Storage Dir
        # self.__cleanup()

    def store_citizenserve(self):
        # Upsert Citizenserve Permit Data
        self.socrata = SocrataClient(
            service_data=self.citizenserve.permits,
            dataset_id=CS_DATASET_ID,
            citizenserve_update_window=CITIZENSERVE_UPDATE_WINDOW
        )
        self.socrata.run()
        return

    @staticmethod
    def __prepare():
        if not os.path.exists(os.path.join(ROOT_DIR, 'tmp')):
            os.mkdir(os.path.join(ROOT_DIR, 'tmp'))
    
    @staticmethod
    def __cleanup():
        if os.path.exists(os.path.join(ROOT_DIR, 'tmp')):
            shutil.rmtree(os.path.join(ROOT_DIR, 'tmp'))
