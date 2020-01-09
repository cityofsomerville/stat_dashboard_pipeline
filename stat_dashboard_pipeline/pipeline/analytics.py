"""
Grooming for Google Analytics API Return

Raw API JSON -> Socrata Storable JSON
"""
import datetime
from datetime import timedelta

from stat_dashboard_pipeline.clients.analytics_client import GoogleAnalyticsClient


class AnalyticsPipeline(GoogleAnalyticsClient):

    def __init__(self):
        self.analytics = {}
        super().__init__()

    # def run(self):
    #     self.raw = self.ga_client.query()
    #     self.groom()

    def groom_analytics(self):
        """
        Make a JSON dump for Socrata
        """
        raw_data = super().query()
        # Groom data
        data = raw_data['reports'][0]['data']['rows']
        date = (datetime.datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        # The query is sorted, just take the top 20
        for visit in data[0:20]:
            url = visit['dimensions'][0]
            dateurl = '{date}:{url}'.format(date=str(date), url=url)
            final_dict = {
                'date': self.format_dates(date),
                'url': url,
                'pageviews': visit['metrics'][0]['values'][0],
                'title': visit['dimensions'][1].replace('| City of Somerville', '').strip(),
            }
            self.analytics[dateurl] = final_dict

    @staticmethod
    def format_dates(date):
        return datetime.datetime.strptime(date, '%Y%m%d')
