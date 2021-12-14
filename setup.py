"""
mppsteel setup script.
For licence information, see licence.txt
"""
import os
from setuptools import setup

# only specify install_requires if not in RTD environment
if os.getenv("READTHEDOCS") == "True":
    INSTALL_REQUIRES = []
else:
    with open("requirements.txt") as f:
        INSTALL_REQUIRES = [line.strip() for line in f.readlines()]

# Basic setup information of the library
setup(
    name="mppsteel",
    version="0.1",
    description="A library for the MPP Steel model",
    author="SYSTEMIQ",
    packages=["mppsteel"],
    python_requires=">=3.8",
    install_requires=INSTALL_REQUIRES,
)
