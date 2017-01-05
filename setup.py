#!/usr/bin/env python
"""A setup module for simple-dataset"""

from os import path
from setuptools import find_packages, setup


def get_version():
    with open("simple_dataset/__init__.py") as f:
        for line in f:
            if line.startswith("__version__"):
                return line.replace("'", "").replace('"', '').split()[-1]
    raise RuntimeError("Could not find the version string in __init__.py")

# Define the constants that describe the package to PyPI
NAME = "simple-dataset"
DESCRIPTION = "A simple format to store many compressed numpy arrays"
with open("README.rst") as f:
    LONG_DESCRIPTION = f.read()
MAINTAINER = "Angelos Katharopoulos"
MAINTAINER_EMAIL = "katharas@gmail.com"
LICENSE = "MIT"

def setup_package():
    setup(
        name=NAME,
        version=get_version(),
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        maintainer=MAINTAINER,
        maintainer_email=MAINTAINER_EMAIL,
        license=LICENSE,
        classifiers=[
            "Intended Audience :: Science/Research",
            "Intended Audience :: Developers",
            "License :: OSI Approved :: MIT License",
            "Topic :: Scientific/Engineering",
            "Programming Language :: Python",
            "Programming Language :: Python :: 2",
            "Programming Language :: Python :: 2.7",
        ],
        packages=find_packages(exclude=["docs", "tests"]),
        install_requires=["numpy"]
    )

if __name__ == "__main__":
    setup_package()
