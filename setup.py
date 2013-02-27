#!/usr/bin/env python

import os
from setuptools import setup

VERSION = open(os.path.join(os.path.dirname(__file__), 'VERSION')).read().strip()

setup(name='drcli',
      version=VERSION,
      description='Command-line tools for docrep',
      author = 'Joel Nothman, schwa lab',
      author_email = 'joel.nothman@gmail.com',
      url='http://schwa.org/git/drcli.git',
      packages=['drcli'],
      install_requires=['argparse', 'brownie'],
      package_data = {
        'drcli': ['plugins/*/*.py'],
      },
      entry_points = {      
        'console_scripts': [
            'dr = drcli.__main__:main',
        ]
      },
      setup_requires=['setuptools_git'],
      license='Apache 2.0',
     )

