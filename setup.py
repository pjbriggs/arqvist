"""Description

Setup script to install archiver

Copyright (C) University of Manchester 2015 Peter Briggs

"""

# Hack to acquire all scripts that we want to
# install into 'bin'
from glob import glob
scripts = []
for pattern in ('archiver.py',):
    scripts.extend(glob(pattern))

# Setup for installation etc
from setuptools import setup
import curatus
setup(name = "archiver",
      version = curatus.get_version(),
      description = 'Utility to perform archiving and curation of NGS data',
      long_description = """Utility to help examine and archive NGS data from
      SOLiD and Illumina sequencing platforms""",
      url = 'https://bitbucket.org/pjbriggs/archiver',
      maintainer = 'Peter Briggs',
      maintainer_email = 'peter.briggs@manchester.ac.uk',
      packages = ['curatus'],
      license = 'Artistic License',
      # Pull in dependencies
      # See http://stackoverflow.com/questions/19738085/why-isnt-setup-py-dependency-links-doing-anything for info on use of 'dependency_links'git+https://bitbucket.org/pjbriggs/auto_process_ngs.git'
      dependency_links=['git+https://github.com/fls-bioinformatics-core/genomics.git',
                        'git+https://bitbucket.org/pjbriggs/auto_process_ngs.git'],
      install_requires = ['genomics-bcftbx',
                          'auto_process_ngs'],
      # Enable 'python setup.py test'
      test_suite='nose.collector',
      tests_require=['nose'],
      # Scripts
      scripts = scripts,
      include_package_data=True,
      zip_safe = False)
