# ENRAM data repository for bird profiles

## Rationale

On the [BALTRAD](http://baltrad.eu/) infrastructure, a [vol2bird](https://github.com/adokter/vol2bird) pipeline generates vertical profiles of birds from weather radar volume scans for over 100 radars. To archive these data, the [European Network for the Radar surveillance of Animal Movement (ENRAM)](http://enram.eu) has set up an open data repository at http://enram.github.io/data-repository.

## Use

### Browsing the data

You can browse the bird profile data at http://enram.github.io/data-repository, which uses the following directory structure and name conventions:

```
structure                                   name convention

└── country                                 two-letter code (ISO 3166-1 alpha-2)
    └── radar                               three-letter code (last 3 letters from the ODIM code)
        └── year                            yyyy
            └── month                       mm
                └── day                     dd
                    └── hour                hh
                        └── data file       corad_vp_yyyymmddhhmmss.h5
```
An overview of the potentially included radars, their location and codes can be found on [this OPERA radar map](http://eumetnet.eu/wp-content/themes/aeron-child/observations-programme/current-activities/opera/database/OPERA_Database/index.html). The bird profile h5 data files follow the [ODIM bird profile format specification](https://github.com/adokter/vol2bird/wiki/ODIM-bird-profile-format-specification).

In addition to separate data files, a zip file is provided for every radar/month combination (`coradyyyymm.zip`), which is more convenient to download. These zips can be found in the year directories (e.g. [here](http://enram.github.io/data-repository/?prefix=nl/dbl/2017/)).

The repository also provides a calendar heatmap to quickly visualize data coverage for a radar/year combination. It can also be found in the year directories (e.g. [here](http://enram.github.io/data-repository/?prefix=nl/dbl/2017/)) and is based on the expected files vs the actual files in the data repository.

### Data access via R

Bird profile files can be downloaded programmatically with the R package [bioRad](https://github.com/adokter/bioRad). [This vignette](https://github.com/adokter/bioRad/blob/master/vignettes/intro_vp.Rmd) explains how to do so.

### File transfer to the data repository

See [file_transfer](file_transfer) for code and documentation.

[![schema](https://cdn.rawgit.com/enram/data-repository/e23d27b4/schema.svg)](schema.svg)

## Contributors

[List of contributors](https://github.com/enram/data-repository/contributors)

## License

[MIT License](LICENSE)
