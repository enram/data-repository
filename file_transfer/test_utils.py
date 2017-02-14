
from file_transfer import *


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
        {'country': 'uk', 'radar': 'jer', 'data_type': 'pvol', 'year': '2015',
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


def test_extract_month_updates():
    example_names = [
        'bezav_pvol_20151109T0000Z.h5',
        'bezav_pvol_20151110T0000Z.h5',
        'bezav_pvol_20151009T0000Z.h5',
        'bezav_pvol_20151010T0000Z.h5',
        'ukjer_pvol_20151010T0000Z.h5',
        'ukjer_pvol_20151001T0000Z.h5',
        'bejab_vp_20161120215000.h5',
        'bejab_vp_20161120225000.h5'
        'bejab_vp_20161120235000.h5'
    ]
    counted = extract_month_updates(example_names)
    print(counted)
    for key in ['bezav 2015-11', 'ukjer 2015-10',
                'bezav 2015-10', 'bejab 2016-11']:
        assert counted[key] == 2


def test_parse_coverage_month():
    example_names = ['bezav 2015-11', 'ukjer 2016-11']
    results = [('be', 'zav', '2015', '11'),
               ('uk', 'jer', '2016', '11')]

    for key, expect_result in zip(example_names, results):
        result = parse_coverage_month(key)
        assert result == expect_result


def test_coverage_to_csv():
    from io import StringIO
    from collections import Counter

    example_counter = Counter({'ukjer 2015-10-01': 2,
                               'bejab 2016-11-06': 166,
                               'bezav 2015-10-11': 8})
    results = ["countryradar,date,vp_files\r\n",
               "bejab,2016-11-06,166\r\n",
               "bezav,2015-10-11,8\r\n",
               "ukjer,2015-10-01,2\r\n"
               ]
    outfile = StringIO()
    coverage_to_csv(outfile, example_counter)
    outfile.seek(0)
    for line in results:
        assert line == outfile.readline()


