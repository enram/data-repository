
import os
from datetime import datetime

from .connectors import FTPConnector, LocalConnector
from .s3enram import S3EnramHandler


class Porter:

    def __init__(self):
        """"""
        self.transferred = []
        self.stalled = []

    def transfer(self):
        raise 'Not implemented'

    def log_transfer(self, succes, filename, verbose=True):
        """store the filename in stalled or transferred report list"""
        if succes:
            self.transferred.append(filename)
            if verbose:
                print("{} is succesfully transferred "
                      "to S3 bucket".format(filename))
        else:
            self.stalled.append(filename)
            if verbose:
                print("{} is not transferred to S3 bucket".format(filename))

    def report(self, reset_file=False, transfertype="Baltrad to S3"):
        """report about the transferred and stalled files

        :param reset_file: if True, a new file is created and an existing
        log file is deleted; if False, text appends
        :param transfertype: Additional text to define the transfer type, 
        provided in the header of the transfer section
        """
        if reset_file:
            file_handler = "w"
        else:
            file_handler = "a"
        with open('./log_file_transfer', file_handler) as outfile:
            outfile.write("-" * 55 + "\n")
            outfile.write("Data transfer at {} from {}:\n".format(
                datetime.now().strftime("%Y-%m-%d %H:%M"), transfertype))
            outfile.write("-" * 55 + "\n")
            outfile.write("\n")
            outfile.write("Files not transferred:\n")
            outfile.write("\n".join(self.stalled))
            outfile.write("\n\n")
            outfile.write("Files succesfully transferred:\n")
            outfile.write("\n".join(self.transferred))
            outfile.write("\n\n\n")


class BaltradToS3(Porter):

    def __init__(self, ftp_url, ftp_username, ftp_pwd,
                 bucket_name, profile_name=None):
        """Port files from Baltrad server to S3

        :param ftp_url: url of the FTP
        :param ftp_username: username of the FTP
        :param ftp_pwd: password of the FTP username
        :param bucket_name: name of the S3 bucket
        :type bucket_name: string
        :param profile_name: name of the AWS profile to use
        """
        Porter.__init__(self)
        self.ftp = FTPConnector(ftp_url, ftp_username, ftp_pwd)
        self.s3 = S3EnramHandler(bucket_name, profile_name)

    def transfer(self, name_match="_vp_", overwrite=False,
                 limit=None, verbose=False):
        """Transfer all current Baltrad files to s3 with the given name_match
        included

        :param name_match: string that should be contained in the file name,
        default _vp_ (bird profile data)
        :param overwrite: If True, overwrite the existing file on the bucket
        :type overwrite: bool
        :param limit: for debugging/testing purposes only, limit the total
        number of transfers
        :param verbose: Make transfer description more extended
        """
        for j, filename in enumerate(self.ftp.list_files(
                name_match=name_match)):

            # get the files from ftp:
            with open(filename, 'bw') as f:
                self.ftp._ftp.retrbinary('RETR ' + filename, f.write)

            upload_succes = self.s3.upload_enram_file(filename,
                                                      overwrite=overwrite)
            self.log_transfer(upload_succes, filename, verbose)
            os.remove(filename)

            if isinstance(limit, int) and j >= limit-1:
                break


class LocalToS3(Porter):

    def __init__(self, filepath, bucket_name,
                 profile_name=None):
        """Port files from local file system to S3

        :param filepath: main project directory to write files to
        :param bucket_name: name of the S3 bucket
        :type bucket_name: string
        :param profile_name: name of the AWS profile to use
        """
        Porter.__init__(self)
        self.local = LocalConnector(filepath)
        self.s3 = S3EnramHandler(bucket_name, profile_name)

    def transfer(self, name_match="_vp_", overwrite=False,
                 limit=None, verbose=False):
        """transfer all profiles in folder to s3

        :param name_match: string that should be contained in the file name,
        default _vp_ (bird profile data)
        :param overwrite: If True, overwrite the existing file on the bucket
        :type overwrite: bool
        :param limit: for debugging/testing purposes only, limit the total
        number of transfers
        :param verbose: Make transfer description more extended
        """
        for j, filepath in enumerate(
                self.local.list_files(name_match=name_match, fullpaths=True)):

            upload_succes = self.s3.upload_enram_file(filepath,
                                                      overwrite=overwrite)
            self.log_transfer(upload_succes, os.path.split(filepath)[-1],
                              verbose)

            if isinstance(limit, int) and j >= limit-1:
                break
