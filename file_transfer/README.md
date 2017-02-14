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
                                └── filename: dkrom_vp_20170114231500.h5


The `baltrad_to_s3.py` file provides the functionality to transfer the hdf5 files from baltrad to the S3 bucket. Furthermore, the coverage file is updated based on the new listing of files and the intermediate monthly zip-files are updated for the set of uploaded files.


## Using the datamover Python module

When using Anaconda, the required Python environment can be created by using the `environment.yml` file. Make sure you set the default channel to `conda-forge` by adapting the settings of the conda installation: `conda config --add channels conda-forge`. 


