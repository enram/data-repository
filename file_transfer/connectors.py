
import os
from glob import glob
from ftplib import FTP

import boto3
import botocore
import requests


class Connector:
    def download_file(self, file):
        """download a file to local drive"""
        raise NotImplemented

    def list_files(self, path=None, name_match="_vp_"):
        """list the files within a given subfolder or relative path"""
        raise NotImplemented


class LocalConnector(Connector):

    def __init__(self, filepath):
        """Connector to handle local directory files, focusing on a specific
        folder subset asa defined by the main filepath

        :param filepath: main directory to work in
        """
        self.filepath = filepath

    def download_file(self, file):
        """"""
        return NotImplemented

    def list_files(self, path=None, name_match="_vp_", fullpaths=False):
        """

        :param path: relative defined to filepath
        :param name_match:
        :param fullpaths: bool define if the full path or only relatve path
        should be returned
        :return: iterator with the matching file names of the path folder and
        subfolders
        """
        if path:
            path_to_list = os.path.join(self.filepath, path, "**",
                                        "*{}*".format(name_match))
        else:
            path_to_list = os.path.join(self.filepath, "**",
                                        "*{}*".format(name_match))
        for subpath in glob(path_to_list, recursive=True):
            if name_match in subpath:
                if fullpaths:
                    yield path
                else:
                    yield os.path.split(path)[-1]


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

    def download_file(self, item):
        """download a github file to the current working directory with the
        same subfolders as represented in the repo

        :param item: response dict from the github API
        """
        response = requests.get(item['download_url'])
        full_name = item['path']
        os.makedirs(os.path.dirname(full_name), exist_ok=True)
        with open(full_name, 'wb') as w:
            w.write(response.content)

    def list_files(self, path=None):
        """
        Returns an iterator that allows you to iterate over all files (i.e.
        the download link of each file) in the given path.

        :param paths: a path on the remote location to find files

        :return: yields a dictionary with the keys download_url, path and name
        """

        url = 'https://api.github.com/repos/{user}/{repo}/contents/{path}'.format(
            user=self.repo_username,
            repo=self.repo_name,
            path='' if not path else path
        )
        response = requests.get(url)
        for f in self._parse_files_from_response(response):
            yield f


class S3Connector(Connector):

    def __init__(self, bucket_name=None):
        """
        Initialize a S3Connector by defining the bucket name. The credentials
        to do so are implicitly derived. For local usage, save
        a ~/.aws/credentials file with  your aws_access_key_id and
        aws_secret_access_key.

        :param bucket_name: name of a bucket
        :type bucket_name: string
        """
        self.bucket_name = bucket_name
        self._connect_to_s3()
        self.bucket = self.s3resource.Bucket(bucket_name)

    def _connect_to_s3(self):
        """
        Private method to connect to the S3 service. Initaties both the
        resource (s3resource) as well as the client (s3client)
        """

        # Create the resource
        self.s3resource = boto3.resource('s3')

        # Get the S3 client from the resource
        self.s3client = self.s3resource.meta.client

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
        Upload a (binary) file to the bucket

        :param filename: name of the file on the local system
        :param object_key: name of the file to be used in the bucket (note
        that subdirectories on S3 should be part of the object key)
        """
        data = open(filename, 'rb').read()
        self.s3client.put_object(Body=data,
                                 Bucket=self.bucket_name,
                                 Key=object_key)

    @staticmethod
    def _strchecklister(input2check):
        """string to list converter

        check if input is a string and make a one element list from string
        """
        if isinstance(input2check, str):
            return [input2check]
        else:
            return input2check

    # TODO: add name_match to the list files
    def list_files(self, path=None):
        """
        List all files in the bucket.
        """

        if path:
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
        else:
            for key in self.bucket.objects.all():
                yield key.key.split("/")[-1]

    def key_exists(self, file_key):
        """check if key is already inside a bucket, rel to path"""

        exists = False
        try:
            self.s3resource.Object(self.bucket_name, file_key).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                exists = False
            else:
                raise
        else:
            exists = True
        return exists


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


