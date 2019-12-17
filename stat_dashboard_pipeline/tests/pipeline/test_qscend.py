import unittest
import datetime

from freezegun import freeze_time
import ddt

from stat_dashboard_pipeline.pipeline.qscend import QScendPipeline


@ddt.ddt
class CitizenServeClientTest(unittest.TestCase):

    def setUp(self):
        self.qscend = QScendPipeline()
        self.qscend.qclient.credentials = {
            'qscend_url': 'https://example.com',
            'qscend_key': '',
        }

    def test_run(self):
        self.qscend.run()
        self.assertEqual(self.qscend.departments, {})

    @freeze_time("2012-01-14 16:30")
    @ddt.data([
        {'request': [
            {
                'id': 1,
                'typeId': 150,
                'displayLastAction': '1/14/2012 4:30 PM',
                'dept': 'Parking',
                'latitude': 0,
                'longitude': 0,
                'status': 1,
                'origin': 'Test Harness',
                'comment': 'PII',
                'reporter': 'PII'
            },
            {
                'id': 2,
                'typeId': 250,
                'displayLastAction': '1/14/2012 4:30 PM',
                'dept': 'Parking',
                'latitude': 0,
                'longitude': 0,
                'status': 1,
                'origin': 'Test Harness',
                'comment': 'PII',
                'reporter': 'PII'
            },
            {
                'id': 3,
                'typeId': 350,
                'displayLastAction': '1/14/2012 4:30 PM',
                'dept': 'Parking',
                'latitude': 0,
                'longitude': 0,
                'status': 1,
                'origin': 'Test Harness',
                'comment': 'PII',
                'reporter': 'PII'
            },
        ]},
        {
            150: {
                'isPrivate': True,
                'ancestor': {
                    'id': 0,
                    'isPrivate': True
                }
            },
            250: {
                'isPrivate': False,
                'ancestor': {
                    'id': 0,
                    'isPrivate': False
                }
            },
            350: {
                'isPrivate': False,
                'ancestor': {
                    'id': 0,
                    'isPrivate': True
                }
            }
        }
    ])
    @ddt.unpack
    def test_groom_changes(self, raw_data, types):
        self.qscend.types = types
        self.qscend.raw = raw_data
        self.qscend.groom_changes()
        # This should only allow request 2 through
        with self.assertRaises(KeyError):
            print(self.qscend.requests[1])
        with self.assertRaises(KeyError):
            print(self.qscend.requests[3])

        request = self.qscend.requests[2]
        self.assertEqual(request['last_modified'], datetime.datetime.now())
        self.assertEqual(request['type'], 250)
        self.assertEqual(request['ancestor'], 0)
        self.assertEqual(request['origin'], 'Test Harness')
        self.assertEqual(request['category'], None)

    @freeze_time("2012-01-14 16:30")
    def test_get_date(self):
        date = self.qscend.get_date('1/14/2012 4:30 PM')
        self.assertEqual(date, datetime.datetime.now())


    @freeze_time("2012-01-14 16:30")
    @ddt.data([
        {'activity': [
            {
                'id': 1,
                'routeId': 'yro, TestHarness, DPW',
                'displayDate': '1/14/2012 4:30 PM',
                'requestId': 2,
                'code': 15,
                'codeDesc': 'Test'
            }
        ]}
    ])
    @ddt.unpack
    def test_groom_activities(self, raw_data):
        self.qscend.raw = raw_data
        self.qscend.groom_activities()

        activity = self.qscend.activities[1]
        self.assertEqual(activity['request_id'], 2)
        self.assertEqual(activity['codeDesc'], 'Test')
        self.assertEqual(activity['route'], "['TestHarness', 'DPW']")
