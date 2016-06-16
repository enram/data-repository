from download_service import GithubConnector

gc = GithubConnector(
    repo_username='adokter',
    repo_name='ODIM-hdf5-test',
    paths=['vp'],
    scratch_folder='tmp'
)
gc.download_all_files()
