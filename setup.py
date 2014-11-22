import os
from setuptools import setup, find_packages

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "federal_data",
    version = "0.0.1",
    author = "Edward A. Roualdes",
    author_email = "edward.roualdes@gmail.com",
    description = ("Download and write, to csv, data from a number of US federal agencies."),
    license = "GPLv3",
    keywords = "federal data, csv",
    url = "https://github.com/roualdes/federal_data",
    packages=find_packages(),
    long_description=read('README.md'),
    classifiers=["Development Status :: 1 - Planning"],
)
