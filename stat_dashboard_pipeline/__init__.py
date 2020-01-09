"""
SomerStat Daily Data Dashboard
Pipeline

Parent pipeline class, with methods favorable to the enduser CLI
"""
import os
import shutil
import logging
import datetime

from stat_dashboard_pipeline.pipeline.citizenserve import CitizenServePipeline
from stat_dashboard_pipeline.pipeline.qscend import QScendPipeline
from stat_dashboard_pipeline.pipeline.analytics import AnalyticsPipeline
from stat_dashboard_pipeline.clients.socrata_client import SocrataClient
from stat_dashboard_pipeline.config import Config, ROOT_DIR
from stat_dashboard_pipeline.pipeline.migrations import QScendMigrations

NAME = "stat_pipeline"

CITIZENSERVE_UPDATE_WINDOW = 30
INIT_UPDATE_WINDOW = 30

LOGGING_FILE = 'stat_pipeline.log'
LOG_LEVEL = logging.DEBUG
logging.basicConfig(
    # filename=os.path.join(os.path.dirname(ROOT_DIR), LOGGING_FILE),
    level=LOG_LEVEL
)


class Pipeline:

    def __init__(self, **kwargs):
        self.citizenserve = CitizenServePipeline()
        self.time_window = kwargs.get('time_window', 1)
        self.qscend = None
        self.analytics_pipeline = AnalyticsPipeline()
        self.socrata_datasets = Config().socrata_datasets

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
        logging.info(
            "[PIPELINE] Init: %s",
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        )
        self.__prepare()

        # QScend
        logging.info("[PIPELINE] Running QScend pipeline")
        self.qscend.run()
        logging.info("[PIPELINE] Storing QScend data")
        self.store_qscend()

        # Citizenserve
        logging.info("[PIPELINE] Running Citizenserve pipeline")
        self.citizenserve.groom_permits()
        logging.info("[PIPELINE] Storing Citizenserve data")
        self.store_citizenserve()

        # GA
        logging.info("[PIPELINE] Running Analytics pipeline")
        self.analytics_pipeline.groom_analytics()
        logging.info("[PIPELINE] Storing Analytics data")
        self.store_analytics()

        # Cleanup Storage Dir
        logging.info("[PIPELINE] Cleaning temp storage")
        self.__cleanup()

    def initialize(self):
        """
        Citizenserve always dumps all data, so no migration needed
        """
        logging.info(
            "[PIPELINE] Initialize CSV files: %s",
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        )
        self.qscend = QScendPipeline(
            time_window=INIT_UPDATE_WINDOW
        )
        self.__prepare()
        # QScend
        self.qscend.run()
        self.citizenserve.groom_permits()
        self.analytics_pipeline.groom_analytics()
        self.dump_to_csv()

    def migrate(self):
        """
        Migrate historical QScend Data
        """
        logging.info(
            "[PIPELINE] Migrate: %s",
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        )
        qsc = QScendMigrations()
        qsc.migrate()

    def store_citizenserve(self):
        # Upsert Citizenserve Permit Data
        socrata = SocrataClient(
            service_data=self.citizenserve.permits,
            dataset_id=self.socrata_datasets['somerville_permits'],
            citizenserve_update_window=CITIZENSERVE_UPDATE_WINDOW
        )
        socrata.run()

    def store_qscend(self):
        # Activities
        # No need to store an empty dataset
        if self.qscend.activities == {}:
            return

        socrata = SocrataClient(
            service_data=self.qscend.activities,
            dataset_id=self.socrata_datasets['somerville_services_activities']
        )
        logging.info('[SOCRATA] Storing QSCend Activities')
        socrata.run()

        # Requests
        socrata.service_data = self.qscend.requests
        socrata.dataset_id = self.socrata_datasets['somerville_services']
        logging.info('[SOCRATA] Storing QSCend Requests')
        socrata.run()

        # Types
        socrata.service_data = self.qscend.types
        socrata.dataset_id = self.socrata_datasets['somerville_services_types']
        logging.info('[SOCRATA] Storing QSCend Types')
        socrata.run()

    def store_analytics(self):
        socrata = SocrataClient(
            service_data=self.analytics_pipeline.analytics,
            dataset_id=self.socrata_datasets['somerville_analytics']
        )
        logging.info('[SOCRATA] Storing Analytics')
        socrata.run()

    def dump_to_csv(self):
        """
        This is an initial 'create CSV' method for initiailizing
        large datasets and instantiating them in the Socrata UI
        """
        # Activites
        socrata = SocrataClient(
            service_data=self.qscend.activities,
            dataset_id=self.socrata_datasets['somerville_services']
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
        # Analytics
        socrata.service_data = self.analytics_pipeline.analytics
        socrata.json_to_csv(filename='analytics.csv')

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
