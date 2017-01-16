# ETL procedure for bird profile data

## Transfer data from BALTRAD FTP server to S3 bucket

1. Login to the BALTRAD FTP server.
2. Navigate to `/data`. This directory contains raw polar volume data (`_pvol_`) and bird profile data (`_vp_`) from the last 3 days.
3. Search for files with `_vp_` in the name (= bird profile data).
4. Parse the file name for metadata, e.g. `dkrom_vp_20170114231500.h5`:
    * `dk`: country code, 2 characters
    * `rom`: radar code, 3 characters
    * `_vp_`: indicates that this is bird profile data, ignore as metadata
    * `2017`: year, 4 characters
    * `01`: month, 2 characters
    * `14`: day, 2 characters
    * `23`: hour, 2 characters
    * `00`: minutes, 2 characters
5. Store file in S3 bucket, using the metadata as the directory structure:
        └── dk
            └── rom
                └── 2017
                    └── 01
                        └── 14
                            └── 23
                                └── dkrom_20170114231500.h5
6. Create new folder if necessary. Don't move file if it already exists.
