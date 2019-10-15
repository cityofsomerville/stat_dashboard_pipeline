import os
from setuptools import setup, Command
# import distutils.cmd
# import distutils.log
# import setuptools
# import subprocess

with open('requirements.txt') as f:
    REQUIREMENTS = f.read().splitlines()

class Linter(Command):
    """A custom command to run Pylint on all Python source files."""
    description = 'run Pylint on Python source files'
    user_options = [
        ('input-dir=', 'i', 'input directory')
    ]
    def initialize_options(self):
        self.input_dir = None
    
    def finalize_options(self):
        if self.input_dir is None:
            self.input_dir = 'stat_dashboard_pipeline/'

    def run(self):
        """Run command."""
        command = 'pylint {input_dir}'.format(input_dir=self.input_dir)
        os.system(command)


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
    cmdclass={
        'lint': Linter
    },
    packages=['stat_dashboard_pipeline'],
    zip_safe=False,
    python_requires='>=3.6',
    test_suite='nose.collector',
    tests_require=['nose', 'pylint'],
    scripts=['bin/stat_pipeline'],
)
