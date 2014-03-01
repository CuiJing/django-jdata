#!/usr/bin/python
#_*_coding:utf-8 _*_
#****************************************************************#
# ScriptName: setup.py
# Author: CuiJing(mrcuijing@gmail.com)
# Create Date: 2014-02-25 15:11
#***************************************************************#

import os
from setuptools import setup, find_packages

#os.environ['DJANGO_SETTINGS_MODULE'] = 'tests.settings'


setup(
    name="django-jdata",
    version='0.1.0',
    url='http://github.com/cuijing/django-jdata',
    author='Cui Jing',
    author_email='mr.cuijing@gmail.com',
    description='jdata module for django ',
    packages=find_packages(exclude='tests'),
    tests_require=[
        'django>=1.4',
        'MySQL-python',
    ],
    #test_suite='runtests.runtests',
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Framework :: Django",
        "Environment :: Web Environment",
        "Operating System :: OS Independent",
        "Natural Language :: English",
    ]
)
