"""Description

Setup script to install arqvist package

Copyright (C) University of Manchester 2015-16 Peter Briggs

"""

# Hack to acquire all scripts that we want to
# install into 'bin'
from glob import glob
scripts = []
for pattern in ('bin/*.py',):
    scripts.extend(glob(pattern))

# Setup for installation etc
from setuptools import setup
import arqvist
setup(
    name = "arqvist",
    version = arqvist.get_version(),
    description = 'Utility for exploration and curation of NGS data',
    long_description = """Utility to help examine and archive NGS data from
    SOLiD and Illumina sequencing platforms""",
    url = 'https://github.com/pjbriggs/arqvist',
    maintainer = 'Peter Briggs',
    maintainer_email = 'peter.briggs@manchester.ac.uk',
    packages = ['arqvist'],
    entry_points = { 'console_scripts':
                     ['arqvist = arqvist.cli:main',]
                 },
    license = 'Artistic License',
    # Pull in dependencies
    install_requires = ['genomics-bcftbx',
                        'auto_process_ngs'],
    # Enable 'python setup.py test'
    test_suite='nose.collector',
    tests_require=['nose'],
    # Scripts
    scripts = scripts,
    include_package_data=True,
    zip_safe = False
)
