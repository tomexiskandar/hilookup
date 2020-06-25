# as seen in https://github.com/pypa/sampleproject/blob/master/setup.py

from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# Arguments marked as "Required" below must be included for upload to PyPI.
# Fields marked as "Optional" may be commented out.

setup(
    name='hilookup',  # Required
    description='A module for fuzzy lookup that provides more controls to users.',
    keywords='data quality tool',  # Optional
    version='0.1.0',
    author='Tomex Iskandar',
    author_email='tomex.iskandar@gmail.com',
    packages=['hilookup']
)