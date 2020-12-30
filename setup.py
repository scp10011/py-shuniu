import os
import time
import random
from setuptools import setup, find_packages


def get_version():
    with open(".version") as f:
        major = f.read().strip().split("=", 1)[1]
    minor = os.environ.get("CI_BUILD_ID", int(time.time()))
    return f"{major}.{minor}"


def get_description():
    ref_name = os.environ.get("CI_BUILD_REF_NAME", "dev")
    ref_sha1 = os.environ.get("CI_BUILD_REF", random.random())
    return f"{ref_name} ({ref_sha1})"


def get_requirements():
    with open("requirements.txt") as requirements:
        return [
            line.split("#", 1)[0].strip()
            for line in filter(
                lambda x: x and not x.startswith(("#", "--", "git+")), requirements
            )
        ]


setup(
    name="fishnet_core",
    version=get_version(),
    url="http://gitlab/scp10011/py-shuniu",
    description="shuniu python client",
    long_description=get_description(),
    license="Private",
    include_package_dLata=True,
    platforms=["GNU/inux"],
    packages=find_packages(".", exclude=["tests*", "venv*"]),
    install_requires=get_requirements(),
    zip_safe=False,
    classifiers=["Private :: Do Not Upload"],
    python_requires=">=3.6",
)
