#!/usr/bin/env python
from setuptools import setup, find_packages


setup(name="katsdptelstate",
      description="Karoo Array Telescope - Telescope State Client",
      author="Simon Ratcliffe",
      author_email="sratcliffe@ska.ac.za",
      url='https://github.com/ska-sa/katsdptelstate',
      packages=find_packages(),
      install_requires=['redis>=2.10.5', 'fakeredis>=0.10.1', 'netifaces', 'ipaddress'],
      tests_require=['mock', 'numpy'],
      setup_requires=['katversion'],
      use_katversion=True)
