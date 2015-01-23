#!/usr/bin/env python
from setuptools import setup, find_packages

setup (
    name = "katsdptelstate",
    version = "trunk",
    description = "Karoo Array Telescope - Telescope State Client",
    author = "Simon Ratcliffe",
    packages = find_packages(),
    package_data={'': ['conf/*']},
    include_package_data = True,
    scripts = [],
    zip_safe = False,
)
