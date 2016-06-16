# This script demonstrates how to use the github connector to download files in
# given path (directory) in the repository.

from download_service import GithubConnector


# initialize the GithubConnector
gc = GithubConnector(
    repo_username='adokter',
    repo_name='ODIM-hdf5-test',
    paths=['vp'],
    local_folder='tmp'
)

# download all files
gc.download_all_files()

# now check in the given local folder that the files are present.
