"""
SomerStat Daily Data Dashboard
Pipeline
"""
NAME = "stat_dashboard_pipeline"

from stat_dashboard_pipeline.pipeline.citizenserve import CitizenServePipeline
from stat_dashboard_pipeline.pipeline.qscend import QScendPipeline

class Pipeline():
    """
    Parent pipeline class, with methods favorable to the enduser CLI

    """
    def __init__(self):
        self.citizenserve = CitizenServePipeline()
        self.qscend = QScendPipeline()

    def run(self):
        """
        Nominal running of pipeline code
        """
        print(self.placeholder_string)
        # TODO: convert to returns, store client next
        self.qscend.run()
        self.citizenserve.run()

    def collect(self):
        return

    def store(self):
        return
