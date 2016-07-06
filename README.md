# Infrastructure & ETLs for bird migration altitude profile data

## Rationale

Setup an infrastructure to harvest and backup bird migration altitude profile data, and offer some data products for easier consumption of that data by users (researchers and applications).

## Schema

[![schema](https://rawgit.com/enram/infrastructure/master/schema.svg)](schema.svg)

## Work packages

1. Setup data repository for hdf5 files on S3
2. Download hdf5 files from BALTRAD server (daily, with 2-day delay)
3. Provide access to data repository via file browser
4. Setup database for data and metadata
5. Process **all** hdf5 files and populate database from scratch (can be repeated)
6. Process **new** hdf5 files and add new data and metadata (daily)
7. Allow access to database via R
8. Allow access to database via PgAdmin (optional)
9. Create derived data for coverage app
10. Develop coverage app
11. Create derived data for flow visualization
12. Adapt flow visualization to work with new data
