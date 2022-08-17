# Script that parse the whole aloft bucket and generate a coverage.csv file
# After discussion with Peter, we decided to keep a simple file count per directory (all listed in a global CSV file, one row per "directory")
from collections import defaultdict
from configparser import ConfigParser
from pathlib import PurePath

import boto3

from constants import CONFIG_FILE
from s3_list_helper import s3list


def main():
    config = ConfigParser()
    config.read(CONFIG_FILE)

    bucket_name = config.get("destination_bucket", "name")

    session = boto3.Session(profile_name="prod")
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket_name)

    counter = defaultdict(int)

    print("Looping over files to count")
    for e in s3list(bucket, "", recursive=True, list_dirs=True, list_objs=True):
        dir = PurePath(e.key).parent
        counter[str(dir)] += 1
        print(".", end="")

    print("Done, will now generate coverage.csv")


if __name__ == "__main__":
    main()
