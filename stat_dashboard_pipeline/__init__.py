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
QS_REQUESTS_ID = '4pyi-uqq6'
QS_ACTIVITIES_ID = 'f7b7-bfkg'
QS_TYPES_ID = 'ikh2-c6hz'
CS_DATASET_ID = 'vxgw-vmky'
CITIZENSERVE_UPDATE_WINDOW = 30
MIGRATION_UPDATE_WINDOW = 30

class Pipeline():
    """
    Parent pipeline class, with methods favorable to the enduser CLI

    """
    def __init__(self, **kwargs):
        self.citizenserve = CitizenServePipeline()
        self.time_window = kwargs.get('time_window', 1)
        self.qscend = None

    def run(self):
        """
        Nominal running of pipeline code
        Products:
            self.qscend.requests
            self.qscend.activities
            self.qscend.types
            self.citizenserve.permits
        """
        self.qscend = QScendPipeline(
            time_window=self.time_window
        )
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
        self.qscend.time_window = MIGRATION_UPDATE_WINDOW
        self.qscend.run()
        self.citizenserve.run()
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
        socrata.dataset_id = QS_REQUESTS_ID
        print('[SOCRATA] Storing QSCend Requests')
        socrata.run()

        # Types
        socrata.service_data = self.qscend.types
        socrata.dataset_id = QS_TYPES_ID
        print('[SOCRATA] Storing QSCend Types')
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
        # Types
        socrata.service_data = self.qscend.types
        socrata.json_to_csv(filename='qscend_types.csv')
        # Permits
        socrata.service_data = self.citizenserve.permits
        socrata.json_to_csv(filename='citizenserve_permits.csv')

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
