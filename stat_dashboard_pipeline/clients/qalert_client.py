import os
import datetime
from datetime import timedelta

import requests

from stat_dashboard_pipeline.auth import Auth


class QAlertClient():

    def __init__(self):
        self.credentials = self.__load_credentials()

    @staticmethod
    def __load_credentials():
        # TODO: build into Auth methods
        auth = Auth()
        return auth.credentials()

    def _generate_response(self, url, querystring):
        headers = {
            'User-Agent': "SomerStatDash/0.0.1",
            'Accept': "*/*",
            'Cache-Control': "no-cache",
            'Host': "somervillema.qscend.com",
            'Connection': "keep-alive",
        }
        auth_params = {
            "key": self.credentials['qscend_key'],
            "output": "JSON",
        }
        querystring.update(auth_params)
        response = requests.get(
            url=url,
            headers=headers,
            params=querystring
        )
        if response.status_code != 200:
            # TODO: Better error handling, TBD
            print('[ERROR] : Qscend API')
            print(response.text)
            return None
        return response.text

    @staticmethod
    def _format_date(time_window=0):
        return requests.utils.quote(
            (datetime.datetime.now() - timedelta(days=time_window)).strftime("%m/%d/%Y")
        )

    def get_by_date_range(self, ticket_id=None, time_window=7):
        """
        Get all tickets from last n dates (default 7)
        -or-
        Get specific ticket activity, for all time
        """
        url = os.path.join(self.credentials['qscend_url'], 'requests', 'get')

        current_date = self._format_date()
        previous_date = self._format_date(time_window)

        querystring = {
            "createDateMax": current_date,
            "createDateMin": previous_date
        }
        if ticket_id is not None:
            querystring['id'] = str(ticket_id)
            querystring['createDateMin'] = None

        return self._generate_response(
            url,
            querystring
        )

    def get_changes(self, time_window=1):
        """
        Find changes to tickets since specific date, which includes new tickets
        Defaulted to 1 by kwarg days of changes
        TODO: This is kind of a hectic pull, so may need some refinement
        """
        url = os.path.join(self.credentials['qscend_url'], 'requests', 'changes')
        querystring = {
            "since": self._format_date(time_window),
            "includeCustomFields": False
        }

        return self._generate_response(
            url,
            querystring
        )

    def get_types(self, type_id=None):
        """
        Get ticket types/cats
        """
        url = os.path.join(self.credentials['qscend_url'], 'types', 'get')
        querystring = {}
        if type_id is not None:
            querystring['id'] = str(type_id)

        return self._generate_response(
            url,
            querystring
        )

    def dump_date_data(self, time_window=1):
        """
        Get data dump for time window (default, last 1 day)
        """
        url = os.path.join(self.credentials['qscend_url'], 'requests', 'dump')
        querystring = {
            "start": self._format_date(time_window),
            "end": self._format_date()
        }

        return self._generate_response(
            url,
            querystring
        )

    def get_departments(self):
        """
        Get Department Titles/IDs
        """
        url = os.path.join(self.credentials['qscend_url'], 'departments', 'get')
        querystring = {}
        return self._generate_response(
            url,
            querystring
        )
