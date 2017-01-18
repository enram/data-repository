
import re


def parse_filename(name):
    """
    parse a BALTRAD file name and return relevant information.

    :param name: string the file name to be parsed. Eventual path and
    extension will be removed.
    :rtype: dict containing relevant information from the file name
    """

    name_regex = re.compile(
        r'([^_]*)_([^_]*)_(\d\d\d\d)(\d\d)(\d\d)T?(\d\d)(\d\d)(?:Z|00)?.*\.h5')

    match = re.match(name_regex, name)
    if match:
        country_radar, data_type, year, \
            month, day, hour, minute = match.groups()
        return {'country': country_radar[:2],
                'radar': country_radar[2:],
                'data_type': data_type,
                'year': year,
                'month': month,
                'day': day,
                'hour': hour}
    else:
        return None

