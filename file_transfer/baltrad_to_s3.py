"""
Baltrad to S3 porting
"""

import sys

from creds import URL, LOGIN, PASSWORD
from connectors import BaltradToS3
from s3handlers import S3Handler
from helper_functions import coverage_to_csv


def main():
    """Run data transfer from Baltrad to S3"""

    # ------------------
    # DATA TRANSFER
    # ------------------

    # Setup the connection of the Baltrad and S3
    btos = BaltradToS3(URL, LOGIN, PASSWORD, "lw-enram")

    # Execute the transfer
    btos.transfer(name_match="_vp_", overwrite=False, limit=None)
    btos.report(reset_file=False, transfertype="Baltrad to S3")

    # ------------------
    # UPDATE COVERAGE
    # ------------------

    # Connecto to S3 client
    s3client = S3Handler("lw-enram")

    # Rerun file list overview to extract the current coverage
    coverage_count = s3client.count_enram_coverage(level='day')
    with open("coverage.csv", 'w') as outfile:
        coverage_to_csv(outfile, coverage_count)
    s3client.upload_file("coverage.csv", "coverage.csv")

    # ----------------------------
    # UPDATE ZIP FILE AVAILABILITY
    # ----------------------------
    # Rerun ZIP handling to create files


if __name__ == "__main__":
    sys.exit(main())
