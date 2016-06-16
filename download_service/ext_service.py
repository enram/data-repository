from os import path
import requests


class GithubConnector():

    def __init__(self, repo_username=None, repo_name=None, paths=None, local_folder=None):
        self.repo_username = repo_username
        self.repo_name = repo_name
        self.paths = paths
        self.folder = local_folder

    def _parse_files_from_response(self, response):
        response_data = response.json()
        for item in response_data:
            if item['type'] == 'file':
                yield item['download_url']

    def list_files(self):
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
        for link in self.list_files():
            name = path.split(link)[-1]
            response = requests.get(link)
            with open(path.join(self.folder, name), 'wb+') as w:
                w.write(response.content)

