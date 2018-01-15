
import os
import shutil
from zipfile import ZipFile
from datetime import datetime
from collections import Counter

from .connectors import S3Connector
from .utils import (parse_filename, extract_month_updates,
                    parse_coverage_month)


class S3EnramHandler(S3Connector):

    def __init__(self, bucket_name, profile_name=None):
        S3Connector.__init__(self, bucket_name, profile_name)

    def upload_enram_file(self, filepath, overwrite=False):
        """Upload a (binary) file to the bucket
        Upload a file with a local filepath (path + filename) to the S3
        bucket, defining if the file should be overwritten if the key already
        exists on the S3 bucket

        :param filepath: full path and file name of the file to upload
        :param overwrite: If True, overwrite the existing file on the bucket
        :type overwrite: boolean
        """

        with open(filepath, 'br') as f:
            filename = os.path.split(filepath)[-1]
            file_info = parse_filename(filename)
            object_location = "/".join([file_info['country'],
                                        file_info['radar'],
                                        file_info['year'],
                                        file_info['month'],
                                        file_info['day'],
                                        file_info['hour'],
                                        filename])

            if (not overwrite) and self.key_exists(object_location):
                return False
            else:
                self.s3client.put_object(Body=f,
                                         Bucket=self.bucket_name,
                                         Key=object_location)
                return True

    def count_enram_coverage(self, level='day'):
        """Count the number of files for each country/radar combination and 
        extract last modified date

        At a given time interval (day, month, year), the available number of
        files in the S3 bucket is counted.

        :param level: day | month | year
        :return: Counter (dict), with the key representing the identifier and
        the values the counts
        """
        file_count = Counter()
        file_most_recent = {}

        for name in self.list_files():
            file_info = parse_filename(name)
            if file_info:
                country_radar = "{}{}".format(file_info["country"],
                                              file_info["radar"])
                file_date = datetime(int(file_info["year"]), 
                                     int(file_info["month"]), 
                                     int(file_info["day"]), 
                                     int(file_info["hour"]), 
                                     int(file_info["minute"]))

                # Get the counts on the required level
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

                # Get the most recent file for each country/radar
                if country_radar in file_most_recent.keys():
                    if file_date > file_most_recent[country_radar]:
                        file_most_recent[country_radar] = file_date
                else:
                    file_most_recent[country_radar] = file_date

        return file_count, file_most_recent

    def create_zip_version(self, keylisting):
        """Collect all keys in the listing in a combined zip folder and
        store the zip on the appropriate S3 bucket location

        Create a zip version of those folders the given keys of the keylisting
        are part of, keys can be defined as a list of keys or as a Counter
        object

        :param keylisting: Counter with the key/counts or a list of keys from
        which the monthly counts will be derived
        """

        if isinstance(keylisting, list):
            keylisting = extract_month_updates(keylisting)

        for levels, count in keylisting.items():
            country, radar, year, month = parse_coverage_month(levels)

            # Download the proper files (all! the files in the
            # respective subdirs)
            current_keys = []
            for key in self.list_files(
                    path="/".join([country, radar, year, month])):
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
            shutil.rmtree(os.path.join(".", file_info['country']))
