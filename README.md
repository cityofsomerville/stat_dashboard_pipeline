# stat_dashboard_pipeline
City of Somerville's Somerstat Dashboard pipeline code.

## Intro
This code hits several endpoints based on clients built to the API specs, grooms and deidentifies data to city spec, and uploads the groomed data into Socrata (and other) endpoints.

## Install
<!-- Note: This is a temporary placeholder until buildout is complete. -->
```
git clone https://github.com/cityofsomerville/stat_dashboard_pipeline.git
cd stat_dashboard_pipeline

# Development
pip install -e .

# Production
pip install .

```

## Run

## Test
```
python setup.py test
```

(c)2019 City of Somerville
