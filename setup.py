#!/usr/bin/env python

import os
from setuptools import setup

def _path(name):
  return os.path.join(os.path.dirname(__file__), name)


VERSION = open(_path('VERSION')).read().strip()
APPS = [l.strip() for l in open(_path('apps.lst'))]

setup(name='drapps',
      version=VERSION,
      description='Additional command-line tools for (Python-oriented) docrep',
      author = 'Joel Nothman',
      author_email = 'joel.nothman@gmail.com',
      url='https://github.com/schwa-lab/dr-apps-python',
      packages=['drapps'],
      install_requires=['libschwa-python', 'msgpack-python >= 0.3'],
      package_data = {
        'drapps': ['plugins/*/*.py'],
      },
      entry_points = {      
        'console_scripts': [
            'dr-{0} = drapps.__main__:main'.format(cmd)
            for cmd in APPS
        ]
      },
      setup_requires=['setuptools_git'],
      license='Apache 2.0',
     )

