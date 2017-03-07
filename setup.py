#!/usr/bin/env python
from setuptools import setup, find_packages


setup(name="katsdptelstate",
      description="Karoo Array Telescope - Telescope State Client",
      author="Simon Ratcliffe",
      author_email="sratcliffe@ska.ac.za",
      url='https://github.com/ska-sa/katsdptelstate',
      packages=find_packages(),
      package_data={'': ['conf/*']},
      include_package_data=True,
      install_requires=['redis', 'fakeredis', 'netifaces', 'numpy'],
      tests_require=['mock'],
      scripts=[],
      zip_safe=False,
      setup_requires=['katversion'],
      use_katversion=True)
