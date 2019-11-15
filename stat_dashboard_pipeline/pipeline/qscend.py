"""
Grooming process for QScend Client

Raw JSON API Dumps -> Socrata Storable JSON
"""
import json
import pprint

from stat_dashboard_pipeline.clients.qalert_client import QAlertClient

class QScendPipeline():

    def __init__(self):
        self.departments = {}
        self.types = {}
        self.activities = {}
        self.requests = {}
        # self.activity = {}
        self.raw = None
        # self.cleaned = None
        self.qclient = QAlertClient()

    def run(self):
        # """
        # Semi-temp master run funct
        # """
        # self.groom_depts()
        # self.groom_types()
        # self.get_type_ancestry()

        # Get Changes
        self.get_changes()
        # self.groom_changes()

        self.infer_activities()

    def get_changes(self):
        self.raw = json.loads(self.qclient.get_changes())
        # For the sake of tidyness, let's delete the unneeded keys
        del self.raw['deleted'] # Empty
        del self.raw['attachment'] # Unusable
        del self.raw['submitter'] # Contains PII
        try:
            del self.raw['comments'] # Can contain PII
        except KeyError:
            pass


    def groom_changes(self):
        """
        Get the data from the QScendAPI Client, munge into a usable dict
        """
        raw_requests = self.raw['request']

        for request in raw_requests:
            # Delete PII, convert to dict keyed on ID
            self.requests[request['id']] = {
                'last_modified': request['displayLastAction'],
                'dept': request['dept'],
                'typeName': request['typeName'],
                'latitude': request['latitude'],
                'longitude': request['longitude'],
                'status': self.get_statuses(request['status']),
                'type': self.types[request['typeId']]
            }
            break
        pprint.pprint(self.requests)
        # return changes

    @staticmethod
    def get_statuses(status_no):
        """
        From QScendAPI docs:
        valid values are 0 (open), 1 (closed), 3 (in progress), and 4 (on hold).
        """
        valid_statuses = {
            0: 'Open',
            1: 'Closed',
            3: 'In Progress',
            4: 'On Hold'
        }
        return valid_statuses[status_no]


    def groom_depts(self):
        raw_depts = json.loads(self.qclient.get_departments())
        for dept in raw_depts:
            self.departments[dept['id']] = dept['name']

    def groom_types(self):
        """
        Call API, get raw types, munge into a dict
        """
        raw_types = json.loads(self.qclient.get_types())
        for q_type in raw_types:
            type_id = q_type['id']
            # Unneeded values,
            # 'id' becomes the key
            # 'priorityValue' is always '2'
            # TODO: 'isPrivate'?
            del q_type['id']
            del q_type['priorityValue']

            # Department name
            if q_type['dept'] is not 0:
                # Add department name
                department = self.departments[q_type['dept']]
                q_type['dept'] = department
            self.types[type_id] = q_type

    def infer_activities(self):
        raw_activities = self.raw['activity']
        for activity in raw_activities:
            print(activity)
            if self.activities.get('code') is not None and self.activities['code'] != activity['codeDesc']:
                print('VALUEERR')
                print(self.activities['code'])
                print(activity['codeDesc'])
            self.activities['code'] = activity['codeDesc']
            # print(activity['code'])
            print('------')

    def get_type_ancestry(self):
        """
        Get types, munge, and get top node parent type
        NOTE: We have to run this AFTER initial grooming, or we'll
        get key errors
        """
        # Run types API, munge to dict
        for key, entry in self.types.items():
            # Add 'ancestor' key/value
            if entry['parent'] is not 0:
                self.types[key]['ancestor'] = self._get_ancestor(self.types[key])

    def _get_ancestor(self, q_type):
        """
        Recurse to find ancestor node
        """
        if q_type['parent'] is 0:
            return q_type
        parent_id = q_type['parent']
        return self._get_ancestor(self.types[parent_id])


if __name__ == '__main__':
    # TODO: Remove
    qcl = QScendPipeline()
    qcl.run()
