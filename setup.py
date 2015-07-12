#!/usr/bin/env python

from distutils.core import setup
import sys

DNSPYTHON_PACKAGE = {
    True: 'dnspython',
    False: 'dnspython3',
}[sys.version_info[0] == 2]

setup(name='thingsbus',
      version='0.2.1',
      description='Thingsbus data broker, input, output, and dispatch.',
      author='Eric Stein',
      author_email='toba@des.truct.org',
      url='https://github.com/eastein/thingsbus/',
      packages=['thingsbus'],
      install_requires=[DNSPYTHON_PACKAGE, 'zmqfan>=0.8', 'msgpack-python', 'six']
     )
