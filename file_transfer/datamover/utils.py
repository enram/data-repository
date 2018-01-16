
import re
import csv

from collections import Counter


class EnramNameParsingError(Exception):
    """
    Exception placeholder for name parse issues
    """
    pass


def parse_filename(name):
    """parse enram bird profile name to individual descriptions dict

    Parse a BALTRAD hdf5 file name and return relevant information as a dict,
    collecting the individual components country, radar, data_type, year,
    month, day, hour, minute.

    File format is according to the following file format,
    ccrrr_vp_yyyymmddhhmmss.h5 (with c the country code two-letter ids and rrr
    the radar three-letter ids), e.g. bejab_vp_20161120235500.h5

    :param name: string the file name to be parsed. Eventual path and
    extension will be removed.
    :return: dict containing relevant information from the file name
    """

    name_regex = re.compile(
        r'([^_]{2})([^_]{3})_([^_]*)_(\d\d\d\d)(\d\d)(\d\d)T?'
        r'(\d\d)(\d\d)(?:Z|00)?.*\.h5')

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
        # not a valid file, can be ignored by other routines
        return None


def extract_month_updates(keylist):
    """count country/radar files on monthly basis in given filekey list

    Loop through all keys in the given list of keys and count the number of
    files in each country/radar/month combinations

    :param keylist: list of keys according to the enram bird profile name,
    e.g. bejab_vp_20161120235500.h5
    :return: Counter (dict) with counts for each country/radar/month
    combination
    """
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
    """utility to parse a Counter with monthly coverage key output

    :param name: Split a name according to the format ccrrr yyy-mm (with cc a 2
     letter abbreviation for the country name and rrr a 3 letter abbreviation
     of the radar name) into the respective groups
    :return: country, radar, year, month string interpretation
    """

    name_regex = re.compile(
        r'([^_]{2})([^_]{3}) (\d\d\d\d)-(\d\d)')
    match = re.match(name_regex, name)
    if match:
        country, radar, year, month = match.groups()
    else:
        raise EnramNameParsingError

    return country, radar, year, month


def coverage_to_csv(csvfile, coverage_count):
    """save counter of dict into a csv file-like object

    To use the functionality, provide a file handler (with open() as
    csvfile: ...)

    :param csvfile: an open file handle (or file-like object)
    :param coverage_count: Counter (dict) object with the key values according
    to the ccrrr yyy-mm format
    """

    fieldnames = ['countryradar', 'date', 'vp_files']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for fileinfo, count in sorted(coverage_count.items()):
        country_radar = fileinfo.split(" ")[0]
        date = fileinfo.split(" ")[1]
        writer.writerow({'countryradar': country_radar,
                         'date': date,
                         'vp_files': count})

def most_recent_to_csv(csvfile, most_recent_file):
    """save dict with the most recent file date into a csv file-like object

    To use the functionality, provide a file handler (with open() as
    csvfile: ...)

    :param csvfile: an open file handle (or file-like object)
    :param most_recent_files: dict object with the key values according
    to the ccrrr format and values a datetime-object
    """

    fieldnames = ['countryradar', 'datetime_latest_data']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for country_radar, last_date in sorted(most_recent_file.items()):
        writer.writerow({'countryradar': country_radar,
                         'datetime_latest_data': last_date.strftime("%Y-%m-%d %H:%M")})                         
