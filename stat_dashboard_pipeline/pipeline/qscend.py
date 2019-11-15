"""
Grooming process for QScend Client

Raw JSON API Dumps -> Socrata Storable JSON
"""
import json
import pprint

from stat_dashboard_pipeline.clients.qalert_client import QAlertClient

class QScendPipeline():

    def __init__(self):
        self.qclient = QAlertClient()
        self.raw = None
        # Intermediate Data
        self.departments = {}
        self.types = {}
        self.activity_codes = {}
        self.activity = {}
        # Final
        self.requests = {}

    def run(self):
        """
        Semi-temp master run funct
        """
        self.groom_depts()
        self.groom_types()
        self.get_type_ancestry()

        # Get Changes
        self.get_changes()
        self.groom_changes()
        self.infer_activity_codes()
        self.groom_activites()
        self.munge_activities_into_requests()
        pprint.pprint(self.requests)

    def get_changes(self):
        """
        Call and clean response from QAlertClient class
        """
        self.raw = json.loads(self.qclient.get_changes())
        # For the sake of tidyness, let's delete the unneeded keys
        # TODO: Date handling
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
        Create usable FE dict
        """
        # TODO: Check origins (ios, call center, QAlert Mobile iOS, Control Panel)
        raw_requests = self.raw['request']

        for request in raw_requests:
            # Ditch PII, convert to dict keyed on ID
            self.requests[request['id']] = {
                'last_modified': request['displayLastAction'], # TODO: Date handling
                'dept': request['dept'],
                'typeName': request['typeName'],
                'latitude': request['latitude'],
                'longitude': request['longitude'],
                'status': self.get_statuses(request['status']),
                'type': self.types[request['typeId']],
                'origin': request['origin']
            }
            # TODO: remove
            break

    def groom_activites(self):
        """
        Create a dict of arrays of dicts, keyed on request ID

        self.activity = {
            request id: [
                {activity},
                {activity}
            ]
        }
        """
        # TODO: (Maybe) routeId / Comment parsing
        # TODO: Date handling
        raw_activities = self.raw['activity']
        for activity in raw_activities:
            # Delete unused
            del activity['attachments']
            del activity['notify']
            del activity['user']
            del activity['files']
            del activity['isEditable']
            act_id = activity['requestId']
            # Get or set extant value
            activity_list = self.activity.setdefault(act_id, [])
            activity_list.append(activity)
            # Sort by ID (appears to increment)
            sorted_list = sorted(activity_list, key=lambda i: (i['id']))
            self.activity[act_id] = sorted_list

    def munge_activities_into_requests(self):
        """
        Add the activities into the requests dict
        TODO: Can be collapsed into above func and self.activity can be deleted
        """
        return

    @staticmethod
    def get_statuses(status_no):
        """
        From QScendAPI docs:
        valid values are 0 (open), 1 (closed), 3 (in progress), and 4 (on hold).
        """
        # TODO: move to config
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
        Call API, get raw types, munge into dict
        """
        raw_types = json.loads(self.qclient.get_types())
        for q_type in raw_types:
            type_id = q_type['id']
            # Unneeded values
            # TODO: 'isPrivate'?
            del q_type['id']
            del q_type['priorityValue']

            # Department name
            if q_type['dept'] is not 0:
                department = self.departments[q_type['dept']]
                q_type['dept'] = department
            self.types[type_id] = q_type

    def infer_activity_codes(self):
        """
        Infer Activity Names from 'activity' return from API
        """
        raw_activities = self.raw['activity']
        for activity in raw_activities:
            if self.activity_codes.get('code') is not None and \
            self.activity_codes['code'] != activity['codeDesc']:
                # TODO: Err handling
                continue
            self.activity_codes[activity['code']] = activity['codeDesc']

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
