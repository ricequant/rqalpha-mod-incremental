#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2017 Ricequant, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import (
    find_packages,
    setup,
)


setup(
    name='rqalpha-mod-incremental',
    version='0.0.6',
    description='rqalpha-mod-incremental',
    packages=find_packages(exclude=[]),
    author='ricequant',
    author_email='public@ricequant.com',
    license='Apache License v2',
    package_data={'': ['*.*']},
    install_requires=[
        'pymongo',
    ],
    zip_safe=False,
)
