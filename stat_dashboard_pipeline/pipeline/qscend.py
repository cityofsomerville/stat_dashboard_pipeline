"""
Grooming process for QScend Client

Raw JSON API Dumps -> Socrata Storable JSON
"""
import json
import datetime

from stat_dashboard_pipeline.clients.qscend_client import QScendClient
from stat_dashboard_pipeline.config import Config


class QScendPipeline():

    def __init__(self, **kwargs):
        self.qclient = QScendClient()
        self.raw = None
        self.time_window = kwargs.get('time_window', 1)
        self.departments = {}
        # Final
        self.requests = {}
        self.activities = {}
        self.types = {}


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
        self.groom_activites()
        self.groom_published_types()

    def get_changes(self):
        """
        Call and clean response from QScendClient class
        """
        self.raw = json.loads(
            self.qclient.get_changes(time_window=self.time_window)
        )
        # Delete the unneeded keys
        del self.raw['deleted'] # Empty
        del self.raw['attachment'] # Unusable
        del self.raw['submitter'] # Contains PII

    def groom_changes(self):
        """
        Get the data from the QScendAPI Client, munge into a usable dict
        Create usable FE dict
        """
        raw_requests = self.raw['request']
        categories = self.get_categories()
        for request in raw_requests:
            # Convert to dict keyed on ID
            try:
                category = categories[str(request['typeId'])]
            except KeyError:
                category = None

            # Only take up "isPrivate: False" requests
            # ('isPrivate' is internal use only)
            try:
                self.types[request['typeId']]
            except KeyError:
                continue
            else:
                if self.types[request['typeId']]['isPrivate'] or \
                    self.types[request['typeId']]['ancestor'] and \
                    self.types[request['typeId']]['ancestor']['isPrivate']:
                    continue

            last_modified = self.get_date(request['displayLastAction'])

            ancestor_id = None
            if self.types[request['typeId']]['ancestor']:
                ancestor_id = self.types[request['typeId']]['ancestor']['id']

            self.requests[request['id']] = {
                'last_modified': last_modified,
                'dept': request['dept'],
                'latitude': request['latitude'],
                'longitude': request['longitude'],
                'status': self.get_statuses(request['status']),
                'type': request['typeId'],
                'ancestor': ancestor_id,
                'origin': request['origin'],
                'category': category
            }

    @staticmethod
    def get_date(date):
        return datetime.datetime.strptime(date, '%m/%d/%Y %I:%M %p')

    def groom_activites(self):
        """
        Create a subtable of activites with a
        FK equivalent keyed on request ID
        """
        raw_activities = self.raw['activity']
        for activity in raw_activities:
            # Delete unused
            # TODO: Move to config
            del activity['attachments']
            del activity['notify']
            del activity['user']
            del activity['files']
            del activity['isEditable']
            del activity['actDate']
            del activity['actDateUnix']

            # Convert to datetime
            action_date = self.get_date(activity['displayDate'])
            activity['action_date'] = action_date
            del activity['displayDate']

            # TODO: Comment parsing
            del activity['comments']

            # Parse Routes
            for route in activity['routeId'].split(','):
                # This is a little dicey, but seems to work in AD2019
                # Names are typically u/n like "gmartin"
                # and departments are usually formatted like "DPWAdmin"
                if route.strip() and route.strip()[0].isupper():
                    activity['route'] = route.strip()
                else:
                    activity['route'] = None
            del activity['routeId']

            self.activities[activity['id']] = {
                'request_id': activity['requestId'],
                'action_date': activity['action_date'],
                'code': activity['code'],
                'codeDesc': activity['codeDesc'],
                'route': activity['route']
            }

    def groom_depts(self):
        raw_depts = json.loads(self.qclient.get_departments())
        for dept in raw_depts:
            self.departments[dept['id']] = dept['name']

    def groom_published_types(self):
        """
        This step is necessary because not
        every day's requests call every type

        """
        final_types = {}
        for key, entry in self.types.items():
            if not self.types[key]['isPrivate']:
                del entry['isPrivate']
                del entry['parent']
                if self.types[key]['ancestor']:
                    entry['ancestor_id'] = self.types[key]['ancestor']['id']
                    entry['ancestor_name'] = self.types[key]['ancestor']['name']
                else:
                    entry['ancestor_id'] = 0
                    entry['ancestor_name'] = None
                del entry['ancestor']
                final_types[key] = entry
        self.types = final_types

    def groom_types(self):
        """
        Call API, get raw types, munge into dict
        """
        raw_types = json.loads(self.qclient.get_types())
        for q_type in raw_types:
            type_id = q_type['id']
            # Unneeded values
            del q_type['id']
            del q_type['priorityValue']

            # Department name
            if q_type['dept'] != 0:
                department = self.departments[q_type['dept']]
                q_type['dept'] = department
            self.types[type_id] = q_type

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
                self.types[key]['ancestor'] = self._get_ancestor(
                    q_type=self.types[key],
                    key=key
                )
            try:
                self.types[key]['ancestor']
            except KeyError:
                self.types[key]['ancestor'] = None


    def _get_ancestor(self, q_type, key):
        """
        Recurse to find ancestor node
        """
        if q_type['parent'] == 0:
            q_type['id'] = key
            return q_type
        parent_id = q_type['parent']

        return self._get_ancestor(
            q_type=self.types[parent_id],
            key=parent_id
        )

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
