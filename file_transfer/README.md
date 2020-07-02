# File transfer

## Introduction

A daily file transfer procedure from the BALTRAD server to the data repository has been developed. The procedure provides the following functional steps:
1. Transfer newly available data files from the BALTRAD server to the data repository. The files in scope are files with `_vp_` in the name (= bird profile data) in the subdirectory `/data`. These files are placed in the [appropriate folder structure](https://github.com/enram/data-repository#browsing-the-data) on Amazon S3, using the metadata in the file names. Files are not transfered if they already exists (default option).
2. Recalculate the monthly data availability for each of the radars (i.e. `coverage.csv` file) providing a count of the number of h5 files present for each radar/day combination. This file serves as the source data for the calendar heatmap. 
3. For each of the radar/month combinations for which new data has been added, the corresponding zip-files (`coradyyyymm.zip`) are regenerated and updated on the data repository. These zips can be found in the year directories (e.g. [here](http://enram.github.io/data-repository/?prefix=nl/dbl/2017/)).

To accommodate the above procedure together with other custom data transfers in between the different entities, a [Python module](https://github.com/enram/data-repository/tree/master/file_transfer/datamover), called `datamover`, has been developed. The module enables a high-level access and data exchange between the BALTRAD FTP server, the Amazon S3 data repository and a local file directory. Hence, the `datamover` module is used by the daily file transfer and can be used for ad-hoc adjustments.

## Using the datamover Python module

When using Anaconda, the required Python environment can be created by using the `environment.yml` file. Make sure you set the default channel to `conda-forge` by adapting the settings of the conda installation: `conda config --add channels conda-forge`. 

The `datamover` Python module contains three main parts:

- `connectors.py`: A set of general purpose classes that are able to connect to a file source (`Connector`), provide consistent handling to enlist the available files (`list_files` method), and download those files (`download_file` method):
  - `LocalConnector`: local file listing
  - `S3Connector`: Contains the additional methods `upload_file` and `key_exists` methods
  - `BaltradFTPConnector`: FTP connection to the BALTRAD server
  - `GithubConnector`: Connection to GitHub repo
- `s3enram.py`: Provides the `S3EnramHandler` class (inherited from `S3Connector`) with dedicated methods for the Amazon S3 bucket for ENRAM
- `transporters.py`: Provides two transporter classes, `BaltradToS3` and `LocalToS3`, with a `transfer` method to transfer files from one file storage to another.

Furthermore, a `utils.py` file provides the appropriate file name parsing and handling. The `baltrad_to_s3.py` file provides the functionality to daily transfer the hdf5 files from BALTRAD to the Amazon S3 bucket. 

The [tutorial_datamover](tutorial_datamover.ipynb) notebook provides a more practical introduction and manual to the `datamover ` code.

## Technical setup BALTRAD to S3 data transfer

The daily transfer of the `vol2bird ` bird profile data from Baltrad to the Amazon S3 bucket is supported by an Amazon EC2 (t2.micro, `eu-west-1c` availability zone) instance with Ubuntu as OS. The Python `datamover` module is installed on the instance and a daily cron job runs the `baltrad_to_s3.py` file, executing the necessary data transfer steps.

### AWS setup S3 bucket

The Amazon S3 bucket has been setup with the name `lw-enram` in EU (Ireland) region. Everyone has the permission to list and view the files, so *Permission* is set to `Everyone: list and view permission` and with the *Bucket Policy*:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AddPerm",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::lw-enram/*"
        }
    ]
}
```

### AWS setup EC2 instance

The Amazon EC2 instance has a dedicated IAM role (`INBO-LW-ENRAM-ETL`) to read and write files to `lw-enram` Amazon S3 Bucket, provided by the following policy, called `enram-s3`:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::lw-enram"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::lw-enram/*"
            ]
        }
    ]
}
```

Furthermore, the Inbound rules of the instance are configured to make sure the EC2 machine is capable of accessing remote FTP servers. On the security group level, HTTP and HTTPS access is enabled. 

### EC2 instance configuration

The following setup is required on the Amazon EC2 Ubuntu machine to deploy the data transfer:

1. Install [Miniconda](https://conda.io/docs/install/quick.html#linux-miniconda-install):

	```bash
	wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
	bash Miniconda3-latest-Linux-x86_64.sh
	rm Miniconda3-latest-Linux-x86_64.sh
	```

2. Get the code from the GitHub repository (home directory of `ubuntu` user)

	```bash
	git clone https://github.com/enram/data-repository.git
	cd data-repository/file_transfer
	```

3. Create a dedicated Python (Conda) environment to run the `datamover` code, by using the Anaconda `environment.yml` [file](https://github.com/enram/data-repository/blob/master/file_transfer/environment.yml), as described [here](https://conda.io/docs/using/envs.html#use-environment-from-file):

	```bash
	conda config --add channels conda-forge
	conda env create -f environment.yml
	```
4. In the `data-repository/file_transfer` folder, make the bash-script that activates the conda environment and starts the file_transfer procedure (called `enram.sh`) executable:

	```bash
	chmod +x enram.sh
	```

5. Setup the cron-job that runs the file transfer (`enram.sh` script) on a daily basis, by executing the command `crontab -e`. An editor will open to add the cron job and some additional settings. Make sure the following items are added to the crontab file:

	1. make bash the used shell
 	2. adapt the path to include the Miniconda environment
	3. add the job to run on a daily basis, at 8 a.m. in the morning

	```
	SHELL = /bin/bash
	PATH = /home/ubuntu/miniconda2/bin:/home/ubuntu/bin:/home/ubuntu/.local/bin:/home/ubuntu/miniconda2/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin

	0 8 * * * bash /home/ubuntu/data-repository/file_transfer/enram.sh &>>  /home/ubuntu/data-repository/file_transfer/cronlog_enram
	```

6. Add the appropriate access rights for the `datamover` to the Baltrad server in the `data-repository/file_transfer` folder, by adding a small Python file, called `creds.py`, with the following content:

	```python
	# FTP
	URL = "odc.baltrad.eu"
	LOGIN = "volbird"
	PASSWORD = "xxxxx" # appropriate password required(!)
	```

**Remark:** In case of trouble, first check any error messages in the `cronlog_enram` file or check the system logs in the `/var/log` folder.

**Remark:** The Amazon EC2 instance is merely an Ubuntu Machine in the AWS cloud. It is therefore possible to replicate this setup on any other server, as long as it is configured properly, especially the access rights to the Amazon S3 bucket.

Since the current implementation is still DEV, an additional backup archive in DEV has been created which can be used to provide a storage when experimenting. Syncing both is most easy done with the AWS CLI:

```
aws s3 sync s3://lw-enram s3://lw-enram-archive --exclude *.tmp --profile lw-enram
```

**Remark:** This EC2 instance has a tendency to run out of inodes because automatic updates leaves a tons of installed kernels, along their full source trees. Background and a manual fix are available in [#58](/../../issues/58), a CloudWatch alarm has also been configured so we are informed by mail when the situation happens again.
