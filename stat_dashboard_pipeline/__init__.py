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
QS_DATASET_ID = 'rhvv-tfhr'
QS_ACTIVITIES_ID = 'qca6-ha4e'
CS_DATASET_ID = 'ec6w-s4am'
CITIZENSERVE_UPDATE_WINDOW = 90

class Pipeline():
    """
    Parent pipeline class, with methods favorable to the enduser CLI

    """
    def __init__(self):
        self.citizenserve = CitizenServePipeline()
        self.qscend = QScendPipeline()

    def run(self):
        """
        Nominal running of pipeline code
        Products:
            self.qscend.requests()
            self.qscend.activities()
            self.citizenserve.permits
            self.citizenserve.types <-- Unused
        """
        self.__prepare()

        # QScend
        self.qscend.run()
        self.store_qscend()

        # Citizenserve
        self.citizenserve.run()
        self.store_citizenserve()
        # Cleanup Storage Dir

        self.__cleanup()

    def migrate(self):
        """
        Citizenserve always dumps all data, so no migration needed
        """
        self.__prepare()
        # QScend
        self.qscend.run()
        # TODO: Send dates as kwargs in to expand range
        self.migrate_qscend()

    def store_citizenserve(self):
        # Upsert Citizenserve Permit Data
        socrata = SocrataClient(
            service_data=self.citizenserve.permits,
            dataset_id=CS_DATASET_ID,
            citizenserve_update_window=CITIZENSERVE_UPDATE_WINDOW
        )
        print('[SOCRATA] Storing Citizenserve')
        socrata.run()

    def store_qscend(self):
        # Activities
        socrata = SocrataClient(
            service_data=self.qscend.activities,
            dataset_id=QS_ACTIVITIES_ID
        )
        print('[SOCRATA] Storing QSCend Activities')
        socrata.run()

        # Requests
        socrata.service_data = self.qscend.requests
        socrata.dataset_id = QS_DATASET_ID
        print('[SOCRATA] Storing QSCend Requests')
        socrata.run()

    def migrate_qscend(self):
        """
        This is an initial 'create CSV' method for migrating
        large datasets and instantiating them in the Socrata UI
        """
        # Activites
        socrata = SocrataClient(
            service_data=self.qscend.activities,
            dataset_id=QS_ACTIVITIES_ID
        )
        socrata.json_to_csv(filename='qscend_activities.csv')
        # Requests
        socrata.service_data = self.qscend.requests
        socrata.json_to_csv(filename='qscend_requests.csv')

    @staticmethod
    def __prepare():
        temp_dir = os.path.join(ROOT_DIR, 'tmp')
        if not os.path.exists(temp_dir):
            os.mkdir(temp_dir)

    @staticmethod
    def __cleanup():
        temp_dir = os.path.join(ROOT_DIR, 'tmp')
        if os.path.exists(temp_dir):
            for file in os.listdir(temp_dir):
                if file != 'README.md':
                    os.remove(os.path.join(temp_dir, file))
