# Data model

A central measurements table that is one join away from additional information.

## measurements (or data?)

Pivoted from `dataset1/` so we have all variables on one row. If the number and types of variables can change over time or if we need to keep `gain` etc., we could have one row for one variable and create a pivoted table like the one below from that.

field | type | from | comment
--- | --- | --- | ---
`fk_settings` | String? |  | E.g. `06410-1`
`radar_id` | String | | Repeated from settings
`fk_datetimes` | Integer |  | E.g. `20160625093015`
`datetime` | Datetime |  | Repeated from datetimes
`fk_heights` | Integer |  | E.g. `1`
`height` | Integer |  | Repeated from heights
`HGHT` | Double? | `dataset1/data1/data` | 
`width` | Double? | `dataset1/data2/data` | 
`ff` | Double | `dataset1/data3/data` | 
`dd` | Double | `dataset1/data4/data` | 
`w` | Double | `dataset1/data5/data` | 
`gap` | Double? | `dataset1/data6/data` | 
`dbz` | Double | `dataset1/data7/data` | 
`eta` | Double | `dataset1/data8/data` | 
`dens` | Double | `dataset1/data9/data` | 
`n` | Integer? | `dataset1/data10/data` | 
`n_dbz` | Integer? | `dataset1/data11/data` | 
`sd_vvp` | Integer? | `dataset1/data12/data` | 
`DBZH` | Double | `dataset1/data13/data` | What to do with "DBZH", "DBZV","TH","TV"?
`n_all` | Integer? | `dataset1/data14/data` | 
`n_dbz_all` | Integer? | `dataset1/data15/data` | 

## settings (or metadata?)

field | type | from | comment
--- | --- | --- | ---
`pk` | String? |  | New record for new combination, e.g. `06410-1` (radar + settings id)
`applies_from` | Datetime | first `/what/date` + `/what/time` | 
`applies_until` | Datetime | last `/what/date` + `/what/time` | Requires update
`what_object` | String | `/what/object` | Needed? Assumed always `VP`?
`what_version` | String | `/what/version` | Version of what?
`what_source` | String | `/what/source` | Need for parsing non-WMO codes?
`radar_id` | String | WMO:code from `/what/source` | What if no WMO code present?
`radar_longitude` | Double | `/where/lon` | 
`radar_latitude` | Double | `/where/lat` | 
`radar_height` | Double | `/where/height` | 
`profile_levels` | Long | `/where/levels` | 
`profile_height_interval` | Double | `/where/interval` | 
`profile_minheight` | Double | `/where/minheight` | 
`profile_maxheight` | Double | `/where/maxheight` | 
`how_beamwidth` | Double | `/how/beamwidth` | 
`how_wavelength` | Double | `/how/wavelength` | 
`how_sd_vvp_thresh` | Double | `/how/sd_vvp_thresh` | 
`how_minrange` | Double | `/how/minrange` | 
`how_maxrange` | Double | `/how/maxrange` | 
`how_minazim` | Double | `/how/minazim` | 
`how_maxazim` | Double | `/how/maxazim` | 
`how_rcs_bird` | Double | `/how/rcs_bird` | 
`how_cluttermap` | String | `/how/cluttermap` | 
`how_task` | String | `/how/task` | 
`how_task_args` | String | `/how/task_args` | 
`how_task_version` | String | `/how/task_version` | 
`how_comment` | String | `/how/comment` | 

## datetimes (or time_intervals)

Table to more easily search on year, month, day, durations, etc, rather than having to parse datetime on the fly. 

field | type | from | comment
--- | --- | --- | ---
`pk` | Integer |  | E.g. `20160625093015`
`datetime` | Datetime |  | E.g. `2016-06-25T09:30:35`
`end_datetime` | Datetime | ? | E.g. `2016-06-25T09:45:35`
`duration` | Integer | ? | E.g. `900` (seconds)
`year` | Integer | From `datetime` | E.g. `2016`
`month` | Integer | From `datetime` | E.g. `06`
`day` | Integer | From `datetime` | E.g. `25`
`hour` | Integer | From `datetime` | E.g. `09`
`minutes` | Integer | From `datetime` | E.g. `30` (not rounded up)

## heights (or height_intervals)

Table to more easily search on specific height intervals. If all height intervals are equal accross all radars, there is no need for this.

field | type | from | comment
--- | --- | --- | ---
`pk` | Integer |  | E.g. `1`
`height` | Double? |  | E.g. `200`
`upper_height` | Double? |  | E.g. `400`
`interval_width` | Double? |  | E.g. `200` m
`height_class` | String | ? | E.g. `groundlevel`
