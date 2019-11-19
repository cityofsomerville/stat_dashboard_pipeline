import os
import time
import datetime
from datetime import timedelta

import paramiko

from stat_dashboard_pipeline.config import Auth
from stat_dashboard_pipeline.definitions import ROOT_DIR


class CitizenServeClient():

    def __init__(self):
        self._credentials = self.__load_credentials()
        self.connection = None
        self.filename = None

    @staticmethod
    def __load_credentials():
        # TODO: build into Auth methods
        auth = Auth()
        return auth.credentials()

    @staticmethod
    def __generate_filename(days_prior=1):
        """
        File nomenclature is similar to: PermitExport09032019.txt
        """
        # TODO: Make robust for multiday failures
        datestring = (datetime.datetime.now() - timedelta(days=days_prior)).strftime("%m%d%Y")
        return 'PermitExport{datestring}.txt'.format(datestring=datestring)

    def __create_connection(self):
        transport = paramiko.Transport(
            sock=(self._credentials['sftp_server'], self._credentials['sftp_port'])
        )
        transport.connect(
            username=self._credentials['sftp_user'],
            password=self._credentials['sftp_pass']
        )
        return paramiko.SFTPClient.from_transport(transport)

    def __remote_path(self):
        return os.path.join(
            self._credentials['sftp_remote_dir'],
            self.filename
        )

    def local_path(self):
        return os.path.join(
            ROOT_DIR,
            'tmp',
            self.filename
        )

    def __file_exists(self, remote_path):
        try:
            self.connection.stat(remote_path)
        except IOError:
            return False
        else:
            return True

    def download(self, retry=5):
        self.filename = self.__generate_filename()
        remote_path = self.__remote_path()
        local_path = self.local_path()

        self.connection = self.__create_connection()

        if self.__file_exists(remote_path) or retry == 0:
            self.connection.get(
                remote_path,
                local_path,
                callback=None
            )
        elif retry > 0:
            time.sleep(5)
            retry = retry - 1
            self.download(retry=retry)
