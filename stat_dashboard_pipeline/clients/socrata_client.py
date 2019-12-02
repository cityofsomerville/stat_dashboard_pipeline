"""
Socrata Data Client

"""
import datetime
from datetime import timedelta

from sodapy import Socrata

from stat_dashboard_pipeline.config import Auth

# TODO: Move to config
T11_DATASET_ID = 'xs7t-pxkc'

class SocrataClient():

    def __init__(self):
        self._credentials = self.__load_credentials()

    @staticmethod
    def __load_credentials():
        # TODO: build into Auth methods
        auth = Auth()
        return auth.credentials()

    def store_json(self):
        return

    def retrieve_json(self):
        client = Socrata(self._credentials['socrata_url'], None)
        week_ago = datetime.datetime.now() - timedelta(days = 7)
        query_string = 'ticket_created_date_time between \'{week_ago}\' and \'{today}\''.format(
            week_ago=week_ago.isoformat(), 
            today=datetime.datetime.now().isoformat()
        )
        results = client.get(
            T11_DATASET_ID, 
            where=query_string,
        )
        print(results)
        return


if __name__ == '__main__':
    scstore = SocrataClient()
    scstore.retrieve_json()
