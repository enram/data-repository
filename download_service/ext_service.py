from os import path
import requests


class GithubConnector():

    def __init__(self, repo_username=None, repo_name=None, paths=None, local_folder=None):
        """
        Initialize a GithubConnector

        :param repo_username: username of the repository owner
        :param repo_name: name of the repository
        :param paths: a list of paths *within* the repository in which files need to be downloaded
        :type paths: list
        :param local_folder: a local folder where files will be downloaded to
        """
        self.repo_username = repo_username
        self.repo_name = repo_name
        self.paths = paths
        self.folder = local_folder

    def _parse_files_from_response(self, response):
        """
        Parses the download_urls from the response and yields them one by one
        """
        response_data = response.json()
        for item in response_data:
            if item['type'] == 'file':
                yield item['download_url']

    def list_files(self):
        """
        Returns an iterator that allows you to iterate over all files (i.e. the download link
        of each file) in the given paths.
        """
        for p in self.paths:
            url = 'https://api.github.com/repos/{user}/{repo}/contents/{path}'.format(
                user=self.repo_username,
                repo=self.repo_name,
                path=p
            )
            response = requests.get(url)
            for f in self._parse_files_from_response(response):
                yield f

    def download_all_files(self):
        """
        Download all files in the given paths in the repository. Files will be saved to the
        local folder that was set at initialization.
        """
        for link in self.list_files():
            name = path.split(link)[-1]
            response = requests.get(link)
            with open(path.join(self.folder, name), 'wb+') as w:
                w.write(response.content)

