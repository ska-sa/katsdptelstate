#!/usr/bin/env python

################################################################################
# Copyright (c) 2015-2019, National Research Foundation (Square Kilometre Array)
#
# Licensed under the BSD 3-Clause License (the "License"); you may not use
# this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   https://opensource.org/licenses/BSD-3-Clause
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import os.path

from setuptools import setup, find_packages


here = os.path.dirname(__file__)
readme = open(os.path.join(here, 'README.rst')).read()
news = open(os.path.join(here, 'NEWS.rst')).read()
long_description = readme + '\n\n' + news
tests_require = ['asynctest', 'async_timeout', 'fakeredis[lua,aioredis]>=1.4.0']

setup(name='katsdptelstate',
      description='Karoo Array Telescope - Telescope State Client',
      long_description=long_description,
      author='MeerKAT SDP team',
      author_email='sdpdev+katsdptelstate@ska.ac.za',
      packages=find_packages(),
      package_data={'': ['lua_scripts/*.lua', 'py.typed']},
      url='https://github.com/ska-sa/katsdptelstate',
      license='Modified BSD',
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: BSD License',
          'Operating System :: OS Independent',
          'Programming Language :: Python',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
          'Topic :: Software Development :: Libraries :: Python Modules',
          'Topic :: Scientific/Engineering :: Astronomy'],
      platforms=['OS Independent'],
      keywords='meerkat ska',
      python_requires='>=3.5',
      setup_requires=['katversion'],
      use_katversion=True,
      install_requires=['redis>=3.3', 'six>=1.12', 'netifaces',
                        'msgpack', 'numpy'],
      extras_require={
          'rdb': ['rdbtools', 'python-lzf'],
          'aio': ['aioredis<2'],
          'test': tests_require
      },
      tests_require=tests_require,
      zip_safe=False     # For py.typed
      )
