"""
SomerStat Daily Data Dashboard
Pipeline
"""
NAME = "stat_dashboard_pipeline"

class Pipeline():
    """
    Parent pipeline class, with methods favorable to the enduser CLI

    """
    def __init__(self):
        self.placeholder_string = 'Hello Somerville'

    def run(self):
        """
        Nominal running of pipeline code
        """
        print(self.placeholder_string)

    def collect(self):
        return

    def groom(self):
        return
