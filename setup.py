# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="cache_to_disk",
    version="4.2.0",
    author="huanghailong",
    author_email="hhl_helonky@163.com",
    description="A local disk caching decorator for Python functions",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Atakey/cache_to_disk",
    packages=find_packages(exclude=['cache_to_disk.tests', "cache_to_disk.disk_cache"]),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        # "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
    ],
    python_requires=">=3.6",
    install_requires=[
        "filelock",
        "orjson",
    ],
)
