# Parcel Data Cleaning Pipeline

## Data Quality Audit

Source files:

- `parcel_readings.csv`: 3,447 rows, 6 columns.
- `parcel_metadata.csv`: 28 rows, 5 columns.

| Issue | Prevalence | Decision | Justification |
| --- | ---: | --- | --- |
| Mixed reading date formats: `YYYY-MM-DD`, `DD/MM/YYYY`, and `DD-Mon-YYYY`. | 2,431 ISO rows, 686 slash-date rows, 330 month-name rows; 100% parseable with explicit rules. | Repair | Dates are valid but ambiguous under automatic parsing, so the pipeline parses each observed format explicitly and treats slash dates as day-first. |
| Inconsistent `sensor_status` spelling/case/whitespace: `OK`, `ok`, ` OK`, `OK `, `Error`, `ERROR`, `error`. | 3,310 non-null rows affected by normalization; clean result is 3,064 `ok`, 246 `error`, 137 `unknown`. | Repair + flag | Status values map cleanly to a small controlled vocabulary; missing statuses are retained as `unknown` and treated as bad sensors. |
| Missing `sensor_status`. | 137 / 3,447 readings, 3.97%. | Flag | Missing sensor state could mean untrusted data, so rows are preserved but marked `unknown` / bad rather than imputed. |
| NDVI outside the physical range `[0, 1]`. | 105 / 3,447 readings, 3.05%; 104 of these are already bad-sensor rows and 1 is marked `ok`. | Flag + repair | Invalid NDVI values are set to null and `had_invalid_ndvi` is set, preserving the row while preventing impossible values from entering analysis. |
| Duplicate `parcel_id` x `date` readings after date repair. | 16 rows in 8 duplicate groups; 8 extra rows. | Repair | Duplicate keys break the requested one-row-per-parcel-date shape, so numeric measurements are averaged and status is collapsed conservatively to the worst observed status. |
| Readings for parcels not found in metadata. | 40 rows for `PARCEL_098` and `PARCEL_099`. | Drop from joined output | These rows cannot be enriched with crop, mill, sowing date, or area, so they are excluded from the metadata-anchored time series. |
| Metadata parcels with no readings. | 3 / 28 parcels: `PARCEL_050`, `PARCEL_051`, `PARCEL_052`. | Flag | The output is a complete daily panel for metadata parcels, so these parcel-date rows are retained with `missing_reading = True`. |
| Missing parcel-date observations in the completed daily panel. | 829 / 4,228 output rows, 19.61%. | Flag | Missing observations are expected in sparse time series; leaving measurement columns null avoids inventing sensor values. |
| Temperature and rainfall range checks. | 0 invalid temperatures outside `[-10, 50]`; 0 negative rainfall values. | Keep | Values pass basic plausibility checks, so no repair is needed. |
| Metadata validity checks: duplicate parcel IDs, invalid sowing dates, non-positive areas. | 0 duplicate parcel IDs, 0 invalid sowing dates, 0 non-positive areas. | Keep | Metadata fields are structurally clean after light normalization of text columns. |

## Pipeline

This implementation uses PySpark DataFrame operations for ingestion, cleaning, joins, aggregation, and analysis. On this Windows machine I used `jdk4py` for a local Java runtime and `spark-submit` to run the job.

Run from this folder:

```powershell
$env:JAVA_HOME='C:\Users\praty\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\Lib\site-packages\jdk4py\java-runtime'
$env:PATH="$env:JAVA_HOME\bin;$env:PATH"
$env:PYSPARK_PYTHON='C:\Users\praty\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe'
$env:PYSPARK_DRIVER_PYTHON=$env:PYSPARK_PYTHON
$env:SPARK_LOCAL_IP='127.0.0.1'
& 'C:\Users\praty\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\Lib\site-packages\pyspark\bin\spark-submit.cmd' --master local[1] clean_pipeline.py
```

The script:

1. Ingests `parcel_readings.csv` and `parcel_metadata.csv`.
2. Cleans dates, statuses, invalid numeric values, and duplicate parcel-date readings.
3. Builds a complete daily panel for all metadata parcels from `2026-01-01` through `2026-05-31`.
4. Joins parcel metadata onto each parcel-date row.
5. Writes `cleaned_parcel_timeseries.csv` and `crop_ndvi_summary.csv`.

Output check: `cleaned_parcel_timeseries.csv` has 4,228 rows, 19 columns, and 0 duplicate `parcel_id` x `date` rows.

## Quick Analysis

Rows where `sensor_status` is not `ok` are ignored. NDVI windows are calendar-day windows relative to each parcel's `sowing_date`: days `-30` through `-1` for before and days `0` through `30` for after.

| crop_type | mean_ndvi_before | mean_ndvi_after | n_parcels |
| --- | ---: | ---: | ---: |
| soybean | 0.1706 | 0.3109 | 4 |
| sugarcane | 0.1783 | 0.3362 | 19 |
| wheat | 0.1761 | 0.3135 | 2 |

All three crop types show higher mean NDVI in the 30 days after sowing than in the 30 days before sowing, which is directionally consistent with early crop establishment or greening after planting. Sugarcane has the strongest support because it contributes 19 parcels, while soybean and especially wheat should be interpreted cautiously because their contributing parcel counts are small.

## Production-Readiness Reflection

If this pipeline ran daily and the dataset was 100x larger, I would change three things:

1. Write distributed partitioned outputs with Spark's native writer to object storage or a table format like Delta/Iceberg instead of collecting the final rows into one local CSV.
2. Add formal schema and expectation checks with clear failure thresholds for date formats, parcel ID referential integrity, duplicate keys, NDVI bounds, and sensor-status vocabulary.
3. Make the pipeline incremental by processing only new or changed reading dates, then upserting into a partitioned parcel-date table.

I would monitor row counts by source date, percent bad or unknown sensor statuses, percent invalid NDVI, duplicate parcel-date counts, metadata join failure rates, missing-reading rates by parcel and mill, and shifts in crop-level NDVI summaries. The most likely silent break is date parsing: a new date format or a locale flip between day-first and month-first could still parse successfully but assign readings to the wrong day.
