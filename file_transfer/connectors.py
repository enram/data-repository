
import os
from datetime import datetime
from glob import glob
from ftplib import FTP
from collections import Counter

import boto3
import botocore
import requests

from helper_functions import parse_filename


class Connector:
    def download_file(self, file):
        raise 'Not implemented'

    def list_files(self, paths):
        raise 'Not implemented'


class GithubConnector(Connector):

    def __init__(self, repo_username=None, repo_name=None):
        """
        Initialize a GithubConnector

        :param repo_username: username of the repository owner
        :param repo_name: name of the repository
        """
        self.repo_username = repo_username
        self.repo_name = repo_name

    @staticmethod
    def _parse_files_from_response(response):
        """
        Parses the download_urls from the response and yields them one by one
        :param response: a JSON response from the Github API that lists files
        in a given directory
        """
        response_data = response.json()
        for item in response_data:
            if item['type'] == 'file':
                yield {
                    'download_url': item['download_url'],
                    'path': item['path'],
                    'name': item['name']
                }

    def download_file(self, file):
        response = requests.get(file['download_url'])
        full_name = file['path']
        os.makedirs(os.path.dirname(full_name), exist_ok=True)
        with open(full_name, 'wb') as w:
            w.write(response.content)

    def list_files(self, paths):
        """
        Returns an iterator that allows you to iterate over all files (i.e.
        the download link of each file) in the given paths.
        :param paths: a list of paths on the remote location to list files from
        """
        for p in paths:
            url = 'https://api.github.com/repos/{user}/{repo}/contents/{path}'.format(
                user=self.repo_username,
                repo=self.repo_name,
                path=p
            )
            response = requests.get(url)
            for f in self._parse_files_from_response(response):
                yield f


class S3Connector(Connector):

    def __init__(self, bucket_name=None):
        """
        Initialize a S3Connector
        :param bucket_name: name of a bucket
        :type bucket_name: string
        """
        self.bucket_name = bucket_name
        self._connect_to_s3()
        self.bucket = self._s3resource.Bucket(bucket_name)

    def _connect_to_s3(self):
        """
        Private method to connect to the S3 service
        """

        # Create the resource
        self._s3resource = boto3.resource('s3')

        # Get the S3 client from the resource
        self.s3client = self._s3resource.meta.client

    def download_file(self, file):
        """
        Download a file with a given object_key from the bucket
        :param file: dict containing information about the file to be downloaded
        :return: nothing
        """
        response = self.s3client.get_object(Bucket=self.bucket_name,
                                            Key=file)
        full_name = file
        os.makedirs(os.path.dirname(full_name), exist_ok=True)
        with open(full_name, 'wb') as w:
            w.write(response['Body'].read())

    def upload_file(self, filename, object_key):
        """
        Upload a file to the bucket
        :param filename: name of the file on the local system
        :param object_key: name of the file to be used in the bucket (note
        that subdirectories on S3 should be part of the object key)
        """
        data = open(filename, 'rb').read()
        self.s3client.put_object(Body=data,
                                 Bucket=self.bucket_name,
                                 Key=object_key)

    def upload_file_enram(self, filepath, overwrite=False):
        """

        :param filepath:
        :param overwrite:
        :return:
        """

        with open(filepath, 'br') as f:
            filename = os.path.split(filepath)[-1]
            file_info = parse_filename(filename)
            object_location = "/".join([file_info['country'],
                                        file_info['radar'],
                                        file_info['year'],
                                        file_info['month'],
                                        file_info['day'],
                                        file_info['hour'],
                                        filename])

            if (not overwrite) and self.key_exists(object_location):
                return False
            else:
                self.s3client.put_object(Body=f,
                                         Bucket=self.bucket_name,
                                         Key=object_location)
                return True

    @staticmethod
    def _strchecklister(input2check):
        """string to list converter

        check if input is a string and make a one element list from string
        """
        if isinstance(input2check, str):
            return [input2check]
        else:
            return input2check

    def list_files(self):
        """iterate over all bucjet files"""
        for key in self.bucket.objects.all():
            yield key.key.split("/")[-1]

    def list_files_path(self, paths):
        """
        List all files in the bucket.
        """

        paths = self._strchecklister(paths)

        for path in paths:
            paginator = self.s3client.get_paginator('list_objects_v2')
            operation_parameters = {'Bucket': self.bucket_name,
                                    'Prefix': path
                                    }
            page_iterator = paginator.paginate(**operation_parameters)
            for page in page_iterator:
                for item in page['Contents']:
                    if item['Key'][-1] != '/':
                        # don't include directories themselves
                        yield item['Key']

    def key_exists(self, file_key):
        """check if key is already inside a bucket, rel to path"""

        exists = False
        try:
            self._s3resource.Object(self.bucket_name, file_key).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                exists = False
            else:
                raise
        else:
            exists = True
        return exists

    def count_enram_coverage(self, level='day'):
        """count the number of file for each day batch"""
        file_count = Counter()

        for name in self.list_files():
            file_info = parse_filename(name)
            if file_info:
                country_radar = "{}{}".format(file_info["country"],
                                              file_info["radar"])
                if level == 'day':
                    date = "-".join(
                        [file_info["year"], file_info["month"],
                         file_info["day"]])
                elif level == 'month':
                    date = "-".join([file_info["year"], file_info["month"]])
                elif level == 'year':
                    date = "-".join([file_info["year"]])
                else:
                    raise Exception('Not a valid count option, choose year,'
                                    'month or day')

                file_count[" ".join([country_radar, date])] += 1

        return file_count


class BaltradFTPConnector(Connector):

    _ftp_connection = None

    def __init__(self, ftp_url=None, ftp_username=None,
                 ftp_pwd=None, subfolder='data'):
        """
        Initialize a GithubConnector

        :param repo_username: username of the repository owner
        :param repo_name: name of the repository
        """
        self._ftp_url = ftp_url
        self._ftp_username = ftp_username
        self._ftp_pwd = ftp_pwd

        self._connect_to_ftp(self._ftp_url, self._ftp_username,
                             self._ftp_pwd, subfolder)

    def _connect_to_ftp(self, url, login, pwd, subfolder):
        """
        Private method to connect to the S3 service
        """
        self._ftp = FTP(host=url, user=login, passwd=pwd)
        self._ftp.cwd(subfolder)

    def __del__(self):
        self._ftp.quit()

    def download_file(self, filename):
        """download a single file

        :param filename:
        :return:
        """
        with open(filename, 'wb') as f:
            self._ftp.retrbinary('RETR ' + filename, f.write)

    def list_files(self, namematch="_vp_"):
        """
        Returns an iterator that allows you to iterate over all files
        (i.e. the filename of each file) in the given paths.

        """
        for fname in self._ftp.nlst():
            if namematch in fname:
                yield fname


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
        """report about the transferred and stalled files"""
        if reset_file:
            file_handler = "w"
        else:
            file_handler = "a"
        with open('./logtest', file_handler) as outfile:
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

    def __init__(self, ftp_URL, ftp_LOGIN, ftp_PASSWORD, bucketname):
        """Tunnel Baltrad server to S3

        :param ftp_URL:
        :param ftp_LOGIN:
        :param ftp_PASSWORD:
        :param bucketname:
        """
        Porter.__init__(self)
        self.ftp = BaltradFTPConnector(ftp_URL, ftp_LOGIN, ftp_PASSWORD)
        self.s3 = S3Connector(bucketname)

    def transfer(self, name_match="_vp_", overwrite=False,
                 limit=None, verbose=False):
        """transfer all current baltrad files to s3 with the given name_match
        included

        :param name_match:
        :param overwrite:
        :param limit:
        :param verbose:
        :return:
        """

        for j, filename in enumerate(self.ftp.list_files(
                namematch=name_match)):

            # get the files from ftp:
            with open(filename, 'bw') as f:
                self.ftp._ftp.retrbinary('RETR ' + filename, f.write)

                upload_succes = self.s3.upload_file_enram(filename,
                                                          overwrite=overwrite)
                self.log_transfer(upload_succes, filename, verbose)

            if isinstance(limit, int) and j >= limit-1:
                break


class LocalToS3(Porter):

    def __init__(self, bucketname, filepath):
        """Tunnel Baltrad server to S3

        :param bucketname:
        :param filepath:
        """
        Porter.__init__(self)
        self.s3 = S3Connector(bucketname)
        self.filepath = filepath

    def list_local_files(self, name_match="_vp_", paths=False):
        """iterate over all bucket files

        :param name_match:
        :param paths:
        :return:
        """

        for path in glob(os.path.join(self.filepath, "**", "*.h5"),
                         recursive=True):
            if name_match in path:
                if paths:
                    yield path
                else:
                    yield os.path.split(path)[-1]

    def transfer(self, name_match="_vp_", overwrite=False,
                 limit=None, verbose=False):
        """transfer all profiles in folder to s3

        :param name_match:
        :param overwrite:
        :param limit:
        :param verbose:
        :return:
        """
        for j, filepath in enumerate(
                self.list_local_files(name_match, paths=True)):

            upload_succes = self.s3.upload_file_enram(filepath,
                                                      overwrite=overwrite)
            self.log_transfer(upload_succes, os.path.split(filepath)[-1],
                              verbose)

            if isinstance(limit, int) and j >= limit-1:
                break
