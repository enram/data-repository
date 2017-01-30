from .helper_functions import parse_filename

# ==============================
# Test helper functions
# ==============================


def test_parse_filename():
    example_names = [
        'bezav_pvol_20151009T0000Z.h5',
        'ukjer_pvol_20151010T0000Z.h5',
        'dkste_vp_20151010T0000Z.h5',
        'bejab_vp_20161120235000.h5'
    ]
    results = [
        {'country': 'be', 'radar': 'zav', 'data_type': 'pvol', 'year': '2015',
         'month': '10', 'day': '09', 'hour': '00', 'minute': '00'},
        {'country': 'uk', 'radar': 'jet', 'data_type': 'pvol', 'year': '2015',
         'month': '10', 'day': '10', 'hour': '00', 'minute': '00'},
        {'country': 'dk', 'radar': 'ste', 'data_type': 'vp', 'year': '2015',
         'month': '10', 'day': '10', 'hour': '00', 'minute': '00'},
        {'country': 'be', 'radar': 'jab', 'data_type': 'vp', 'year': '2016',
         'month': '11', 'day': '20', 'hour': '23', 'minute': '50'}
    ]

    for i, name in enumerate(example_names):
        expected_result = results[i]
        result = parse_filename(name)
        for key in ['country', 'data_type', 'day', 'hour', 'minute',
                    'month', 'radar', 'year']:
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
