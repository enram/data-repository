"""
Baltrad to S3 porting
"""

import sys


from creds import URL, LOGIN, PASSWORD
import datamover as dm


def main():
    """Run data transfer from Baltrad to S3"""

    # ------------------
    # DATA TRANSFER
    # ------------------

    # Setup the connection of the Baltrad and S3
    btos = dm.BaltradToS3(URL, LOGIN, PASSWORD, "lw-enram")

    # Execute the transfer
    btos.transfer(name_match="_vp_", overwrite=False, limit=None)
    btos.report(reset_file=False, transfertype="Baltrad to S3")

    # ------------------
    # UPDATE COVERAGE
    # ------------------

    # Connect to S3 client
    s3client = dm.S3EnramHandler("lw-enram")

    # Rerun file list overview to extract the current coverage
    coverage_count = s3client.count_enram_coverage(level='day')
    with open("coverage.csv", 'w') as outfile:
        dm.coverage_to_csv(outfile, coverage_count)
    s3client.upload_file("coverage.csv", "coverage.csv")

    # ----------------------------
    # UPDATE ZIP FILE AVAILABILITY
    # ----------------------------
    # Rerun ZIP handling of S3 for the transferred files, given by report
    s3client.create_zip_version(btos.transferred)


if __name__ == "__main__":
    sys.exit(main())
