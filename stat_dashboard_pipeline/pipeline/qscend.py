"""
Grooming process for QScend Client

Raw JSON API Dumps -> Socrata Storable JSON
"""
import json

from stat_dashboard_pipeline.clients.qscend_client import QScendClient
from stat_dashboard_pipeline.config import Config

class QScendPipeline():

    def __init__(self):
        self.qclient = QScendClient()
        self.raw = None
        # Intermediate Data
        self.departments = {}
        self.types = {}
        self.activity_codes = {}
        self.categories = self.get_categories()
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

    def get_changes(self):
        """
        Call and clean response from QScendClient class
        """
        self.raw = json.loads(self.qclient.get_changes())
        # For the sake of tidyness, let's delete the unneeded keys
        del self.raw['deleted'] # Empty
        del self.raw['attachment'] # Unusable
        del self.raw['submitter'] # Contains PII

    def groom_changes(self):
        """
        Get the data from the QScendAPI Client, munge into a usable dict
        Create usable FE dict
        """
        # TODO: Check origins (ios, call center, QAlert Mobile iOS, Control Panel, etc.)
        raw_requests = self.raw['request']

        for request in raw_requests:
            # Ditch PII, convert to dict keyed on ID
            try:
                category = self.categories[str(request['typeId'])]
            except KeyError:
                category = None

            # TODO: Date handling
            self.requests[request['id']] = {
                'last_modified': request['displayLastAction'],
                'dept': request['dept'],
                # 'typeName': request['typeName'],
                'latitude': request['latitude'],
                'longitude': request['longitude'],
                'status': self.get_statuses(request['status']),
                'type': self.types[request['typeId']],
                'origin': request['origin'],
                'category': category
            }

    def groom_activites(self):
        """
        Create a dict of arrays of dicts, keyed on request ID

        self.requests = {
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
            # TODO: Move to config
            del activity['attachments']
            del activity['notify']
            del activity['user']
            del activity['files']
            del activity['isEditable']
            act_id = activity['requestId']
            try:
                self.requests[act_id]
            except KeyError:
                continue
            # Get or set extant value
            activity_list = self.requests[act_id].setdefault('activity', [])
            activity_list.append(activity)
            # Sort by ID (IDs appear to increment in QScend)
            sorted_list = sorted(activity_list, key=lambda i: (i['id']))
            self.requests[act_id]['activity'] = sorted_list

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
            if q_type['dept'] != 0:
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
            if entry['parent'] != 0:
                self.types[key]['ancestor'] = self._get_ancestor(self.types[key])

    def _get_ancestor(self, q_type):
        """
        Recurse to find ancestor node
        """
        if q_type['parent'] == 0:
            return q_type
        parent_id = q_type['parent']
        return self._get_ancestor(self.types[parent_id])

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

    @staticmethod
    def get_categories():
        """
        These are inhereted from the prior repo, and can
        be updated in 'config/qscend_cat_id_key.json'
        """
        config = Config()
        return config.qscend_categories()
