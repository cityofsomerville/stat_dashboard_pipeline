import json
import datetime
import logging

from stat_dashboard_pipeline.pipeline.qscend import QScendPipeline
from stat_dashboard_pipeline.clients.socrata_client import SocrataClient
from stat_dashboard_pipeline.config import Config

class QScendMigrations(QScendPipeline, SocrataClient):

    def __init__(self):
        self.current_year = int(
            datetime.datetime.now().strftime('%y')
        )
        self.start_year = 15
        self.socrata_datasets = Config().socrata_datasets
        super().__init__()

    def migrate(self):
        super().groom_depts()
        # If 403/Forbidden, the IP isn't whitelisted
        if self.departments == {}:
            logging.error('UNwhitelisted IP - Contact QScend Support')
            return

        super().groom_types()
        super().get_type_ancestry()
        self.migrate_by_date()

    def migrate_by_date(self):
        for year in range(self.start_year, self.current_year):
            for month in range(1, 13):
                start = '{month}/1/{year}'.format(month=str(month), year=str(year))
                if month < 12:
                    end = '{month}/1/{year}'.format(month=str(month+1), year=str(year))
                else:
                    end = '1/1/{year}'.format(year=str(year+1))
                self.retrieve_data_dump(start, end)
                logging.info('[ MIGRATE ] {%s} : {%s}', start, end)
                if not self.raw or not self.raw['request']:
                    continue
                # Groom Requests
                super().groom_changes()
                # Upsert
                self.upsert_data(
                    dataset=self.requests,
                    dataset_id=self.socrata_datasets['somerville_services']
                )
                # Groom Activities
                super().groom_activities()
                # Upsert
                self.upsert_data(
                    dataset=self.activities,
                    dataset_id=self.socrata_datasets['somerville_services_activities']
                )
                logging.info('[ MIGRATE ] Complete : {%s}', end)



    def retrieve_data_dump(self, start, end):
        """
        Start Date: 6/1/15
        """
        try:
            self.raw = json.loads(
                super().dump_date_data(
                    start=start,
                    end=end
                )
            )
        except TypeError:
            return
        # Delete the unneeded keys
        del self.raw['reqcustom'] # Empty
        del self.raw['attachment'] # Unusable
        del self.raw['submitter'] # Contains PII
        del self.raw['deleted'] # Contains PII

    def upsert_data(self, dataset, dataset_id):
        self.service_data = dataset
        self.dataset_id = dataset_id
        super().upsert()
