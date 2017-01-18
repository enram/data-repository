from .helper_functions import *
from .ext_service import *

# ==============================
# Test helper functions
# ==============================

def test_parse_filename():
    example_names = [
        'bezav_pvol_20151009T0000Z.h5',
        'ukjer_pvol_20151010T0000Z.h5',
        'dkste_vp_20151010T0000Z.h5'
    ]
    results = [
        {'radar_name': 'bezav', 'data_type': 'pvol', 'date_time': '201510090000'},
        {'radar_name': 'ukjer', 'data_type': 'pvol', 'date_time': '201510100000'},
        {'radar_name': 'dkste', 'data_type': 'vp', 'date_time': '201510100000'},
    ]
    for i, name in enumerate(example_names):
        expected_result = results[i]
        result = parse_filename(name)
        for key in ['radar_name', 'data_type', 'date_time']:
            assert result[key] == expected_result[key]



# ==============================
# Test connectors with external services
# ==============================

def test_list_files_from_github():
    """
    This test contains no assertions. It's obviously not a great test. It prints out the files returned
    by the GithubConnector so you can manually check this.
    :return:
    """
    gc = GithubConnector(repo_username='adokter',
                         repo_name='ODIM-hdf5-test',
                         paths=['vp'])
    for f in gc.list_files():
        print(f)
