"""
Google Analytics v4
"""

import os
import pprint

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

from stat_dashboard_pipeline.config import Config, ROOT_DIR

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']

class GoogleAnalyticsClient():
    def __init__(self):
        self.credentials = Config().credentials
        self.ga_credentials = Config().ga_credential_file
        self.client = None
        # return

    def run(self):
        self.connect()
        return self.query()

    def connect(self):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.ga_credentials,
            SCOPES
        )
        # Build the service object
        self.client = build(
            'analyticsreporting',
            'v4',
            credentials=credentials
        )

    def query(self):
        return self.client.reports().batchGet(
            body={
                'reportRequests': [{
                    'viewId': self.credentials['ga_view_id'],
                    'dateRanges': [{'startDate': '2daysAgo', 'endDate': 'today'}],
                    'dimensions': [{'name': 'ga:pagePath'}],
                    'metrics': [{
                        'expression': 'ga:pageviews'
                    }],
                    "orderBys": [{
                        "fieldName": "ga:pageviews",
                        "sortOrder": "DESCENDING"
                    }]
                }]
            }
        ).execute()

if __name__ == '__main__':
    gacx = GoogleAnalyticsClient()
    response = gacx.run()
    pprint.pprint(response)
