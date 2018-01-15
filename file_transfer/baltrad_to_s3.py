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

    # ---------------------------------------------
    # UPDATE COVERAGE AND MOST RECENT FILE DATETIME
    # ---------------------------------------------

    # Connect to S3 client
    s3client = dm.S3EnramHandler("lw-enram")

    # Rerun file list overview to extract the current coverage
    coverage_count, most_recent = s3client.count_enram_coverage(level='day')

    # Save the coverage information on S3
    with open("coverage.csv", 'w') as outfile:
        dm.coverage_to_csv(outfile, coverage_count)
    s3client.upload_file("coverage.csv", "coverage.csv")
    
    # Save the last provided radar file information on S3
    with open("radars.csv", 'w') as outfile:
        dm.most_recent_to_csv(outfile, most_recent)
    s3client.upload_file("radars.csv", "radars.csv")

    # ----------------------------
    # UPDATE ZIP FILE AVAILABILITY
    # ----------------------------
    # Rerun ZIP handling of S3 for the transferred files, given by report
    s3client.create_zip_version(btos.transferred)


if __name__ == "__main__":
    sys.exit(main())
