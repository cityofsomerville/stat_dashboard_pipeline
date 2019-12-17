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
## Credentials
Copy the `config/sample_auth.yaml` file and rename it to `auth.yaml`. Put the credentials in the file within the empty quotation marks and save.

## Run
```
stat_pipeline
```

## Migrate
**NOTE:** Does not store in Socrata
To initialize the data for the datasets: Simple one-off queries that dump to CSV files for upload to the Somerville Socrata instance via the UI. 
```
stat_pipeline -m 
# OR
stat_pipeline --migrate
```

## Run Unit Tests
**NOTE:** These should run as acceptance tests in github
```
python setup.py test
```

## Lint
**NOTE:** These should run as acceptance tests in github
```
python setup.py lint
```


(c)2019 City of Somerville
