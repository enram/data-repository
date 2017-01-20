
import os
import shutil
from zipfile import ZipFile
from collections import Counter

from connectors import Connector, S3Connector
from helper_functions import (parse_coverage_month, parse_filename,
                              extract_month_updates)


class S3Handler(S3Connector):

    def __init__(self, bucket_name=None):
        S3Connector.__init__(self, bucket_name)

    def count_enram_coverage(self, level='day'):
        """count the number of file for each day batch"""
        file_count = Counter()

        for name in self.list_files():
            file_info = parse_filename(name)
            if file_info:
                country_radar = "{}{}".format(file_info["country"],
                                              file_info["radar"])
                if level == 'day':
                    date = "-".join(
                        [file_info["year"], file_info["month"],
                         file_info["day"]])
                elif level == 'month':
                    date = "-".join([file_info["year"], file_info["month"]])
                elif level == 'year':
                    date = "-".join([file_info["year"]])
                else:
                    raise Exception('Not a valid count option, choose year,'
                                    'month or day')

                file_count[" ".join([country_radar, date])] += 1

        return file_count

    def create_zip_version(self, keylisting):
        """create a zip version of those folders the given keys are part of,
        keys can be defined as a list of keys or as a counter object"""

        if isinstance(keylisting, list):
            keylisting = extract_month_updates(keylisting)

        for levels, count in keylisting.items():
            country, radar, year, month = parse_coverage_month(levels)

            # Download the proper files (all! the files in the
            # respective subdirs)
            current_keys = []
            for key in self.list_files_path(
                    paths="/".join([country, radar, year, month])):
                self.download_file(key)
                current_keys.append(key)

            # Create a zip folder
            zip_name = "".join([country, radar, year, month, ".zip"])
            with ZipFile(zip_name, 'w') as ziph:
                for key in current_keys:
                    path_info, fname = os.path.split(key)
                    file_info = parse_filename(fname.split("/")[-1])
                    ziph.write(
                        os.path.join(".", file_info['country'],
                                     file_info['radar'], file_info['year'],
                                     file_info['month'], file_info['day'],
                                     file_info['hour'], fname),
                        os.path.join(file_info['day'], file_info['hour'],
                                     fname))

            # Save the ZIP-file on S3
            key_name = "/".join(
                [file_info['country'], file_info['radar'],
                 file_info['year'], zip_name])
            self.upload_file(filename=zip_name, object_key=key_name)
            print("Saved ", zip_name, "on S3 bucket.")

            # Remove the ZIP file locally
            os.remove(zip_name)

            # Remove the affiliated files/folders of the downloads
            shutil.rmtree(os.path.join(".", file_info['country'],
                                       file_info['radar'], file_info['year'],
                                       file_info['month']))
            os.removedirs(os.path.join(".", file_info['country'],
                                       file_info['radar'], file_info['year']))