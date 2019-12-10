# stat_dashboard_pipeline
City of Somerville's Somerstat Dashboard pipeline code.

## Intro
This code hits several endpoints based on clients built to the API specs, grooms and deidentifies data to city spec, and uploads the groomed data into Socrata (and other) endpoints.

## Install
<!-- NOTE: This may be replaced by a pypi package, TBD. -->
```
git clone https://github.com/cityofsomerville/stat_dashboard_pipeline.git
cd stat_dashboard_pipeline

# Development
pip install -e .

# Production
pip install .

```

## Run
```
stat_pipeline
```

## Migrate
To initialize the data for the datasets: Large historical queries and long run time. 
#### WARNING - Does not store in Socrata - only makes temporary CSV files for upload to the Somerville Socrata instance via the UI
```
stat_pipeline -m 
# OR
stat_pipeline --migrate
```

## Test
```
python setup.py test
```

## Lint
```
python setup.py lint
```


(c)2019 City of Somerville
