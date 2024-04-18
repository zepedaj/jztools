# !/usr/bin/env python

from setuptools import setup, find_packages

with open("README.md") as f:
    long_description = f.read()

setup(
    name="jztools",
    packages=find_packages(".", exclude=["tests"]),
    version="0.1.1",
    description="General python utilies",
    install_requires=["pygments", "numpy"],
    author="Joaquin Zepeda",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/zepedaj/jztools",
)
