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
from stat_dashboard_pipeline.pipeline.migrations import QScendMigrations
from stat_dashboard_pipeline.config import Config, ROOT_DIR

NAME = "stat_pipeline"

CITIZENSERVE_UPDATE_WINDOW = 30
INIT_UPDATE_WINDOW = 30

LOGGING_FILE = 'stat_pipeline.log'
logging.basicConfig(
    filename=os.path.join(os.path.dirname(ROOT_DIR), LOGGING_FILE),
    level=logging.INFO
)


class Pipeline(Config):

    def __init__(self, **kwargs):
        self.time_window = kwargs.get('time_window', 1)
        self.qscend = None
        self.citizenserve = None
        self.analytics_pipeline = AnalyticsPipeline()
        super().__init__()

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
        self.citizenserve = CitizenServePipeline(
            update_window=CITIZENSERVE_UPDATE_WINDOW
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
        self.store(
            endpoints=[
                {
                    'service_data': self.qscend.activities,
                    'dataset_id': self.socrata_datasets['somerville_services_activities']
                },
                {
                    'service_data': self.qscend.requests,
                    'dataset_id': self.socrata_datasets['somerville_services']
                },
                {
                    'service_data': self.qscend.types,
                    'dataset_id': self.socrata_datasets['somerville_services_types']
                }
            ]
        )

        # Citizenserve
        logging.info("[PIPELINE] Running Citizenserve pipeline")
        self.citizenserve.groom_permits()
        logging.info("[PIPELINE] Storing Citizenserve data")
        self.store(
            endpoints=[{
                'service_data': self.citizenserve.permits,
                'dataset_id': self.socrata_datasets['somerville_permits']
            }]
        )

        # GA
        logging.info("[PIPELINE] Running Analytics pipeline")
        self.analytics_pipeline.groom_analytics()
        logging.info("[PIPELINE] Storing Analytics data")
        self.store(
            endpoints=[{
                'service_data': self.analytics_pipeline.analytics,
                'dataset_id': self.socrata_datasets['somerville_analytics']
            }]
        )

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
        self.citizenserve = CitizenServePipeline()

        self.__prepare()

        # QScend
        self.qscend.run()
        self.dump_to_csv(
            service_data=self.qscend.activities,
            filename='qscend_activities.csv'
        )
        self.dump_to_csv(
            service_data=self.qscend.requests,
            filename='qscend_requests.csv'
        )
        self.dump_to_csv(
            service_data=self.qscend.activities,
            filename='qscend_types.csv'
        )

        # Citizenserve
        self.citizenserve.groom_permits()
        self.dump_to_csv(
            service_data=self.citizenserve.permits,
            filename='citizenserve_permits.csv'
        )

        # Analytics
        self.analytics_pipeline.groom_analytics()
        self.dump_to_csv(
            service_data=self.analytics_pipeline.analytics,
            filename='analytics.csv'
        )

    @staticmethod
    def dump_to_csv(service_data, filename):
        """
        This is an initial 'create CSV' method for initiailizing
        large datasets and instantiating them in the Socrata UI
        """
        socrata = SocrataClient(
            service_data=service_data
        )
        socrata.json_to_csv(filename=filename)

    @staticmethod
    def migrate():
        """
        Migrate historical QScend Data
        """
        logging.info(
            "[PIPELINE] Migrate: %s",
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        )
        qsc = QScendMigrations()
        qsc.migrate()

    @staticmethod
    def store(endpoints):
        for dataset in endpoints:
            if not dataset['service_data'] or dataset['service_data'] == {}:
                return
            socrata = SocrataClient(
                service_data=dataset['service_data'],
                dataset_id=dataset['dataset_id'],
            )
            socrata.upsert()

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
