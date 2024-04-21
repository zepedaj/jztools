# !/usr/bin/env python

from setuptools import setup, find_packages

with open("README.md") as f:
    long_description = f.read()

setup(
    name="jztools",
    packages=find_packages(".", exclude=["tests"]),
    version="0.1.6",
    description="General python utilies",
    scripts=["scripts/isotest", "scripts/isorun"],
    install_requires=[
        "pygments",
        "numpy",
        "climax",
        "tqdm",
        "coloredlogs",
        "SQLAlchemy",
        "xerializer",
        "plotly",
        "freezegun",
        "torch",
    ],
    author="Joaquin Zepeda",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/zepedaj/jztools",
)
