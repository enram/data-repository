# Infrastructure & ETLs for bird migration altitude profile data

## Rationale

Setup an infrastructure to harvest and backup bird migration altitude profile data, and offer some data products for easier consumption of that data by users (researchers and applications).

## Schema

[![schema](https://rawgit.com/enram/infrastructure/master/schema.svg)](schema.svg)

## Work packages

1. Setup data repository for hdf5 files on S3
2. Develop script to download hdf5 files from BALTRAD server (daily, with 2-day delay)
3. Setup file browser to access hdf5 files
4. Create database model for data and metadata
4. Setup database for data and metadata
5. Develop script with option to process **all** hdf5 files and populate database from scratch (can be repeated) or **new** hdf5 files and add data and metadata (daily)
7. Setup access to database via R
8. Setup access to database via PgAdmin (optional)
9. Develop script to create derived data for coverage app
10. Develop coverage app
11. Develop script to create derived data for flow visualization
12. Adapt flow visualization to work with new data
