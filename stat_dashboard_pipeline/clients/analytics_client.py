"""
Google Analytics v4
"""

from apiclient.discovery import build # pylint: disable=E0401
from oauth2client.service_account import ServiceAccountCredentials

from stat_dashboard_pipeline.config import Config

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']


class GoogleAnalyticsClient(Config):

    def __init__(self):
        self.ga_client = None
        super().__init__()

    def connect(self):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.ga_credential_file,
            SCOPES
        )
        # Build the service object
        self.ga_client = build(
            'analyticsreporting',
            'v4',
            credentials=credentials,
            cache_discovery=False
        )

    def query(self):
        if not self.ga_client:
            self.connect()
        return self.ga_client.reports().batchGet(
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
