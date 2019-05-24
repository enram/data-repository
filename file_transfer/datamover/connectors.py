
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
        """Initialize a Local file path connector

        Connector to handle local directory files, focusing on a specific
        folder subset as defined by the main filepath

        :param filepath: main project directory to look into
        """
        self.filepath = filepath

    def download_file(self, file):
        """Not relevant for this connector"""
        raise NotImplemented

    def list_files(self, path=None, name_match="_vp_", fullpaths=False):
        """list the files within a given subfolder or relative path

        Returns an iterator that allows you to iterate over all files in the
        given path, optionally only those with a specific name match

        :param path: relative defined to filepath
        :param name_match: string that should be contained in the file name,
        default _vp_ (bird profile data)
        :param fullpaths: bool define if the full path or only relative paths
        should be returned
        :return: yields the matching file names of the path folder and
        subfolders
        """
        if path:
            path_to_list = os.path.join(self.filepath, path, "**",
                                        "*{}*".format(name_match))
        else:
            path_to_list = os.path.join(self.filepath, "**",
                                        "*{}*".format(name_match))
        for subpath in glob(os.path.abspath(path_to_list), recursive=True):
            if name_match in subpath:
                if fullpaths:
                    yield subpath
                else:
                    yield os.path.split(subpath)[-1]


class GithubConnector(Connector):

    def __init__(self, repo_username=None, repo_name=None):
        """Initialize a GithubConnector

        Connector to handle GitHub repository

        :param repo_username: username of the repository owner
        :param repo_name: name of the repository
        """
        self.repo_username = repo_username
        self.repo_name = repo_name

    @staticmethod
    def _parse_files_from_response(response):
        """GitHub response parse function
        Parses the download_urls from the response and yields them one by one

        :param response: a JSON response from the GitHub API that lists files
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
        """download GitHub file
        Download a GitHub file to the current working directory with the
        same subfolders as represented in the repo

        :param item: response dict from the GitHub API
        """
        response = requests.get(item['download_url'])
        full_name = item['path']
        os.makedirs(os.path.dirname(full_name), exist_ok=True)
        with open(full_name, 'wb') as w:
            w.write(response.content)

    def list_files(self, path=None):
        """list the files within a given subfolder or relative path

        Returns an iterator that allows you to iterate over all files (i.e.
        the download link of each file) in the given path.

        :param path: a path on the remote location to find files
        :type path: string
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

    def __init__(self, bucket_name=None, profile_name=None):
        """Initialize a Connector to an Amazon S3 bucket

        Initialize a S3Connector by defining the bucket name. The AWS
        credentials to do so are implicitly derived. For local usage, save
        a ~/.aws/credentials file with your aws_access_key_id and
        aws_secret_access_key.

        :param bucket_name: name of the S3 bucket
        :type bucket_name: string
        :param profile_name: name of the AWS profile to use
        """
        self.bucket_name = bucket_name
        self._connect_to_s3(profile_name)
        self.bucket = self.s3resource.Bucket(bucket_name)

    def _connect_to_s3(self, profile_name):
        """S3 client and resource connection
        Private method to connect to the S3 service. Initaties both the
        resource (s3resource) as well as the client (s3client)
        """

        # Create the resource
        session = boto3.Session(profile_name=profile_name)
        self.s3resource = session.resource('s3')

        # Get the S3 client from the resource
        self.s3client = self.s3resource.meta.client

    def download_file(self, file):
        """download S3 bucket file
        Download a file with a given object_key from the bucket

        :param file: dict containing information about the file to download
        :return: nothing
        """
        response = self.s3client.get_object(Bucket=self.bucket_name,
                                            Key=file)
        full_name = file
        os.makedirs(os.path.dirname(full_name), exist_ok=True)
        with open(full_name, 'wb') as w:
            w.write(response['Body'].read())

    def upload_file(self, filename, object_key):
        """Upload a (binary) file to the bucket
        Upload a file with a local filename to the S3 bucket providing an
        object_key from

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

        Check if input is a string and make a one element list from string

        :type input2check: str|list
        """
        if isinstance(input2check, str):
            return [input2check]
        else:
            return input2check

    # TODO: add name_match to the list files
    def list_files(self, path=None):
        """list the files within a given path
        Returns an iterator that allows you to iterate over all files in the
        Bucket or those files with a given path in the prefix (subfolders)

        :param path: a path of the S3 Bucket to find files
        :type path: string
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
        """Check the existence of a file
        Check if a file, represented by the file key is already inside the
        bucket. Note that the concept of folder is in S3 just a name
        convention, so provide the subfolder enlisting as file_key

        :param file_key: full file subdirectory and name listing
        :type file_key: string
        :return: True|false
        """

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


class FTPConnector(Connector):

    _ftp_connection = None

    def __init__(self, ftp_url=None, ftp_username=None,
                 ftp_pwd=None, subfolder='data'):
        """Initialize a Connector to a FTP drive
        Initialize a Connection to an FTP drive or a specific subfolder of the
        FTP drive under consideration

        :param ftp_url: url of the FTP
        :param ftp_username: username of the FTP
        :param ftp_pwd: password of the FTP username
        :param subfolder: optional subfolder listing to set working directory (
        or None)
        """
        self._ftp_url = ftp_url
        self._ftp_username = ftp_username
        self._ftp_pwd = ftp_pwd

        self._connect_to_ftp(self._ftp_url, self._ftp_username,
                             self._ftp_pwd, subfolder)

    def _connect_to_ftp(self, url, login, pwd, subfolder=None):
        """Private method to connect to the FTP drive
        """
        self._ftp = FTP(host=url, user=login, passwd=pwd)
        if subfolder:
            self._ftp.cwd(subfolder)

    def __del__(self):
        self._ftp.quit()

    def download_file(self, filename):
        """Download a single file
        Download a file with a given filename from FTP

        :param filename:
        :type filename: string
        """
        with open(filename, 'wb') as f:
            self._ftp.retrbinary('RETR ' + filename, f.write)

    def list_files(self, name_match="_vp_"):
        """list the files within the current working directory
        Returns an iterator that allows you to iterate over all files in the
        current working directory, optionally only those with a specific name match

        :param name_match: string that should be contained in the file name,
        default _vp_ (bird profile data)
        :return: yields the file names
        """
        for fname in self._ftp.nlst():
            if name_match in fname:
                yield fname
