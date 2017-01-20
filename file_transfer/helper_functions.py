
import re
import csv

from collections import Counter


def parse_filename(name):
    """
    parse a BALTRAD file name and return relevant information.

    :param name: string the file name to be parsed. Eventual path and
    extension will be removed.
    :rtype: dict containing relevant information from the file name
    """

    name_regex = re.compile(
        r'([^_]{2})([^_]{3})_([^_]*)_(\d\d\d\d)(\d\d)(\d\d)T?(\d\d)(\d\d)(?:Z|00)?.*\.h5')

    match = re.match(name_regex, name)
    if match:
        country, radar, data_type, year, \
            month, day, hour, minute = match.groups()
        return {'country': country,
                'radar': radar,
                'data_type': data_type,
                'year': year,
                'month': month,
                'day': day,
                'hour': hour,
                'minute': minute}
    else:
        return None


def extract_month_updates(keylist):
    """loop through all keys and get the """
    file_count = Counter()

    for name in keylist:
        file_info = parse_filename(name)
        if file_info:
            country_radar = "{}{}".format(file_info["country"],
                                          file_info["radar"])
            date = "-".join([file_info["year"], file_info["month"]])
            file_count[" ".join([country_radar, date])] += 1
    return file_count


def parse_coverage_month(name):
    """utility to parse a monthly coverage key output"""

    name_regex = re.compile(
        r'([^_]{2})([^_]{3}) (\d\d\d\d)-(\d\d)')
    match = re.match(name_regex, name)
    country, radar, year, month = match.groups()

    return country, radar, year, month


def coverage_to_csv(coverage_count, filename='coverage.csv'):
    """save counter of dict into a csv file"""
    with open(filename, 'w') as csvfile:
        fieldnames = ['countryradar', 'date', 'vp_files']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for fileinfo, count in coverage_count.items():
            country_radar = fileinfo.split(" ")[0]
            date = fileinfo.split(" ")[1]
            writer.writerow({'countryradar': country_radar,
                             'date': date,
                             'vp_files': count})