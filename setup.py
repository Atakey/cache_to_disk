# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    packages=find_packages(exclude=['cache_to_disk.tests', "cache_to_disk.disk_cache"]),
)
