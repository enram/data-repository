"""
Baltrad to S3 porting
"""

import sys

from creds import URL, LOGIN, PASSWORD
from connectors import BaltradToS3
from helper_functions import coverage_to_csv


def main():
    """Run data transfer from Baltrad to S3"""
    # Setup the connection of the Baltrad and S3
    btos = BaltradToS3(URL, LOGIN, PASSWORD, "lw-enram")

    # Execute the transfer
    btos.transfer(name_match="_vp_", overwrite=False, limit=None)
    btos.report(reset_file=False, transfertype="Baltrad to S3")

    # Get the current coverage
    coverage_count = btos.count_enram_coverage()
    coverage_to_csv(coverage_count, filename='coverage.csv')
    btos.upload_file("coverage.csv", "coverage.csv")


if __name__ == "__main__":
    sys.exit(main())
