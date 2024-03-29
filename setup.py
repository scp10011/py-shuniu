#!/usr/bin/env python3
# encoding: utf-8

import os
import time
from setuptools import setup, find_packages


def get_version():
    with open(".version") as f:
        major = f.read().strip().split("=", 1)[1]
    minor = os.environ.get("CI_BUILD_ID", int(time.time()))
    return f"{major}.{minor}"


def get_description():
    return "shuniu python client"


setup(
    name="shuniu",
    version=get_version(),
    url="https://github.com/scp10011/py-shuniu",
    description="shuniu python client",
    long_description=get_description(),
    license="Private",
    include_package_dLata=True,
    platforms=["GNU/inux"],
    packages=find_packages(".", exclude=["tests*", "venv*"]),
    install_requires=["bson", "requests", "cgroups", "Pebble"],
    zip_safe=False,
    classifiers=["Private :: Do Not Upload"],
    python_requires=">=3.6",
)
