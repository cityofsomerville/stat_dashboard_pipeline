"""
Google Analytics v4
"""

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

from stat_dashboard_pipeline.config import Config, ROOT_DIR

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']


class GoogleAnalyticsClient():

    def __init__(self):
        self.credentials = Config().credentials
        self.ga_credentials = Config().ga_credential_file
        self.client = None

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
            credentials=credentials,
            cache_discovery=False
        )

    def query(self):
        return self.client.reports().batchGet(
            body={
                'reportRequests': [{
                    'viewId': self.credentials['ga_view_id'],
                    'pageSize': 50,
                    'samplingLevel': 'LARGE',
                    'dateRanges': [{'startDate': 'yesterday', 'endDate': 'yesterday'}],
                    'dimensions': [{'name': 'ga:pagePath'}, {'name': 'ga:pageTitle'}],
                    'metrics': [{
                        'expression': 'ga:pageviews',
                    }],
                    "orderBys": [{
                        "fieldName": "ga:pageviews",
                        "sortOrder": "DESCENDING"
                    }],
                }]
            }
        ).execute()
