#!/usr/bin/env python

from distutils.core import setup

setup(name='splunktospan',
      version='0.1',
      packages=['splunktospan'],
      install_requires=[
          "splunk-sdk",
          "python-dateutil",
          "opentracing",
      ])
