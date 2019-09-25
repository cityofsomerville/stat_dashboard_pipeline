import os
import datetime
from datetime import timedelta

import requests

from stat_dashboard_pipeline.auth import Auth


class QAlertClient():

    def __init__(self):
        self._credentials = self.__load_credentials()
    
    def __load_credentials(self):
        # TODO: build into Auth methods
        auth = Auth()
        return auth.credentials()

    def generate_url(self):
        return

    def get_by_date(self):
        """
        # Connection looks like:
        url = self._credentials['qscend_url'] + "/qalert/api/v1/requests/get/"
        querystring = {
            "key": self._credentials['qscend_key'],
            "output": "JSON",
            "createDateMin": "9/16/19"
        }
        headers = {
            'cache-control': "no-cache",
            }

        response = requests.get(
            url, 
            headers=headers, 
            params=querystring
        )

        # querystring for single ID activity:
        querystring = {
            "key": self._credentials['qscend_key'],
            "output": "JSON",
            "id": ${ID_NUMBER},
            "activity": "true"
        }
        """
        print(self._credentials)
        # current_date = requests.utils.quote(datetime.datetime.now().strftime("%m/%d/%Y"))
        # previous_date = requests.utils.quote((datetime.datetime.now() - timedelta(days=10)).strftime("%m/%d/%Y"))

        # url = os.path.join(
        #     self._credentials['qscend_url'], 'qalert/api/v1/requests',
        #     'dump',
        #     '?start={current_date}&end={previous_date}"&key={api_key}'.format(
        #         current_date=current_date,
        #         previous_date=previous_date,
        #         api_key=self._credentials['qscend_key']
        #     )
        # )
        # print(url)
        # requests.get()
        return

    def get_changes(self):
        return

    def get_types(self, type_id):
        """
        Get ticket types/cats
        """
        if type_id is not None:
            return
        return
    
    def get_ticket_activity(self, ticket_id):
        """
        Get specific activity for ID
        """
        return
    

if __name__ == '__main__':
    qac = QAlertClient()
    qac.get_by_date()