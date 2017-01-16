# File transfer

## BALTRAD to S3

This procedure describes how bird profile data should be transferred from the BALTRAD FTP server to an AWS S3 bucket.

1. Connect to the BALTRAD FTP server.
2. Navigate to `/data`, which contains polar volume (`_pvol_`) and bird profile data (`_vp_`) from the last 3 days.
3. Search for files with `_vp_` in the name (= bird profile data).
4. Parse the file name for metadata, e.g. `dkrom_vp_20170114231500.h5`:

    * **country**: 2 characters `dk`
    * **radar**: 3 characters `rom`
    * ignore `_vp_`
    * **year**: 4 characters `2017`
    * **month**: 2 characters `01`
    * **day**: 2 characters `14`
    * **hour**: 2 characters `23`
    * **minutes**: 2 characters `00`

5. Store each file in the AWS S3 bucket, using the metadata as the directory structure. The hour directory will generally contain 4 files (every 15 minutes). Create new directories when necessary. Don't transfer the file if it already exists.

        └── country
            └── radar
                └── year
                    └── month
                        └── day
                            └── hour
                                └── filename: dkrom_20170114231500.h5

