import os
import time
import datetime
from datetime import timedelta
import logging

import paramiko

from stat_dashboard_pipeline.config import Config, ROOT_DIR


class CitizenServeClient():

    def __init__(self):
        self.credentials = Config().credentials
        self.connection = None
        self.filename = None

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
            sock=(self.credentials['sftp_server'], self.credentials['sftp_port'])
        )
        transport.connect(
            username=self.credentials['sftp_user'],
            password=self.credentials['sftp_pass']
        )
        return paramiko.SFTPClient.from_transport(transport)

    def __remote_path(self):
        return os.path.join(
            self.credentials['sftp_remote_dir'],
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
        # Check for already downloaded day file
        if os.path.exists(local_path):
            return

        self.connection = self.__create_connection()
        logging.info('[CITIZENSERVE_CLIENT] Starting Download')
        if self.__file_exists(remote_path) or retry == 0:
            logging.info("[CITIZENSERVE_CLIENT] File download")
            self.connection.get(
                remote_path,
                local_path,
                callback=None
            )
        elif retry > 0:
            time.sleep(5)
            logging.info("[CITIZENSERVE_CLIENT] Retry file download")
            retry = retry - 1
            self.download(retry=retry)
        else:
            logging.error("[CITIZENSERVE_CLIENT] File download failed")
