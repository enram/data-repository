from os import path


def parse_filename(name):
    """
    parse a BALTRAD file name and return relevant information.

    :param name: the file name to be parsed. Eventual path and extension will be removed.
    :type name: string
    :rtype: dict containing relevant information from the file name
    """
    basename = path.split(name)[-1]
    basename_txt = path.splitext(basename)[0]
    parts = basename_txt.split('_')
    dates_info = parts[2]
    return {
        'radar_name': parts[0],
        'data_type': parts[1],
        'date_time': parts[2],
        'year': dates_info[:4],
        'month': dates_info[4:6],
        'day': dates_info[6:8],
        'hour': dates_info[8:10]
    }

