# stat_dashboard_pipeline
City of Somerville's Somerstat Dashboard pipeline code.

## Intro
This code hits several endpoints based on clients built to the API specs, grooms and deidentifies data to city spec, and uploads the groomed data into prepared Socrata endpoints.

This is the companion repo to the [stat_dashboard_frontend](https://github.com/cityofsomerville/stat_dashboard_frontend)

## Issues
Please file found bugs or other issues here in the github repo, it's maintained and monitored. Would you like to contribute a fix? Please do!

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

## Initialize
**NOTE:** Does not store in Socrata
To initialize the data for the datasets: Simple one-off queries that dump to CSV files for upload to the Somerville Socrata instance via the UI. 
```
stat_pipeline -i
```

## Migrate
Run a complete historical migration of QScend activities (takes some time)
```
stat_pipeline -m
```

## Run Unit Tests
**NOTE:** These also run as acceptance tests against pull requests in github
```
python setup.py test
```

## Lint
**NOTE:** These run as acceptance tests against pull requests in github
```
python setup.py lint
```


(c)2019 City of Somerville
