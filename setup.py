# !/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="jztools",
    packages=find_packages(".", exclude=["tests"]),
    version="0.1.0",
    description="Staging package for common tools",
    install_requires=["pygments"],
    author="Joaquin Zepeda",
)
