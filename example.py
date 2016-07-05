# This script demonstrates how to use the github connector to download files in
# given path (directory) in the repository.

from data_repository import GithubConnector, S3Connector

# initialize the GithubConnector
gc = GithubConnector(
    repo_username='adokter',
    repo_name='ODIM-hdf5-test'
)

# download all files
# gc.download_all_files(paths=['vp'], local_folder='tmp')

# now check in the given local folder that the files are present.

s3c = S3Connector(bucket_name='enram-data-repository')
s3c.download_all_files(paths=['test'], local_folder='tmp')
