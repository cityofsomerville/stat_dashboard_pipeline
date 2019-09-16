import os
import sys
import paramiko
import yaml

CONFIG = os.path.join(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(
                os.path.abspath(__file__)
            )
        )
    ),
    'config.yaml'
)
# print(CONFIG)

class SFTPClient():

    def __init__(self): #, host, port, username, password):
        # self.host = host
        # self.port = port
        # self.username = username
        # self.password = password
        self._connection = None

    def load_credentials(self):
        with open(CONFIG, 'r') as stream:
            try:
                creds = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
                return
        print(creds)

    def create_connection(self):
        print(self.host)
        transport = paramiko.Transport(sock=(self.host, self.port))
        transport.connect(username=self.username, password=self.password)
        self._connection = paramiko.SFTPClient.from_transport(transport)

    def file_exists(self, remote_path):
        return
        # try:
        #     print('remote path : ', remote_path)
        #     self._connection.stat(remote_path)
        # except IOError, e:
        #     if e.errno == errno.ENOENT:
        #         return False
        #     raise
        # else:
        #     return True

    def download(self, remote_path, local_path, retry=5):
        return
        # if self.file_exists(remote_path) or retry == 0:
        #     self._connection.get(remote_path, local_path,
        #                          callback=None)
        # elif retry > 0:
        #     time.sleep(5)
        #     retry = retry - 1
        #     self.download(remote_path, local_path, retry=retry)

if __name__ == '__main__':
    sftpclient = SFTPClient(
        # host=SFTP_SERVER, 
        # port=SFTP_PORT, 
        # username=SFTP_USER, 
        # password=SFTP_PASS
    )
    sftpclient.load_credentials()

    # sftpclient.create_connection()
