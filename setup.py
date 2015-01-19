#!/usr/bin/env python

from distutils.core import setup

setup(name='thingsbus',
      version='0.0.2c1',
      description='Thingsbus data broker, input, output, and dispatch.',
      author='Eric Stein',
      author_email='toba@des.truct.org',
      url='https://github.com/eastein/thingsbus/',
      packages=['thingsbus'],
      install_requires=['dnspython', 'zmqfan>=0.7', 'msgpack-python']
     )
