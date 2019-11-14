"""
Grooming process for QScend Client

Raw JSON API Dumps -> Socrata Storable JSON
"""
import json
import pprint

from stat_dashboard_pipeline.clients.qalert_client import QAlertClient

class QScendPipeline():

    def __init__(self):
        self.types = {}
        self.raw = None
        self.cleaned = None
        self.qclient = QAlertClient()

    def run(self):
        """
        Semi-temp master run funct
        """
        # self.types = self.get_types()
        # self.get_type_ancestry()
        self.get_and_dump_changes()

    def get_and_dump_changes(self):
        """
        Get the data from the QScendAPI Client, munge into a usable dict
        """
        raw_changes = json.loads(self.qclient.get_changes())
        # for key, entry in raw_changes.items():
            # print(key)
        pprint.pprint(raw_changes)
        # return changes

    def get_type_ancestry(self):
        """
        Get types, munge, and get top node parent type
        """
        # Run types API, munge to dict
        for key, entry in self.types.items():
            # Add 'ancestor' key/value
            if entry['parent'] is not 0:
                self.types[key]['ancestor'] = self._get_ancestor(self.types[key])

    def get_types(self):
        """
        Call API, get raw types, munge into a dict
        """
        raw_types = json.loads(self.qclient.get_types())
        return_types = {}
        for q_type in raw_types:
            type_id = q_type['id']
            # Unneeded values,
            # 'id' becomes the key
            # 'priorityValue' is always '2'
            # TODO: 'isPrivate'?
            # TODO: 'dept'
            del q_type['id']
            del q_type['priorityValue']
            return_types[type_id] = q_type
        return return_types

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
