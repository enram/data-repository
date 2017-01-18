"""
Baltrad to S3 porting
"""

import sys

from creds import URL, LOGIN, PASSWORD
from connectors import BaltradToS3


def main():
    """Run data transfer from Baltrad to S3"""
    # Setup the connection of the Baltrad and S3
    btos = BaltradToS3(URL, LOGIN, PASSWORD, "lw-enram")

    # Execute the transfer
    btos.transfer(name_match="_vp_", overwrite=False, limit=None)

    btos.report(reset_file=False, transfertype="Baltrad to S3")


if __name__ == "__main__":
    sys.exit(main())
