from setuptools import setup

with open('requirements.txt') as f:
    REQUIREMENTS = f.read().splitlines()

setup(
    name='Stat Dashboard Pipeline',
    description='SomerStat Dashboard data pipeline ingest and upload',
    version='0.1',
    install_requires=REQUIREMENTS,
    url='',
    author='Pulp',
    author_email='info@itspulp.com',
    license='MIT',
    include_package_data=True,
    packages=['stat_dashboard_pipeline'],
    zip_safe=False,
    python_requires='>=3.6',
    test_suite='nose.collector',
    tests_require=['nose'],
    scripts=['bin/stat_pipeline'],
)
