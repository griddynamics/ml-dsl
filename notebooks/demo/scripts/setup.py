#!/usr/bin/python

from setuptools import setup, find_packages

REQUIRED_PACKAGES = ['Keras==2.0.4','matplotlib==2.2.4','seaborn==0.9.0']

setup(
    name='trainer',
    version='1.0',
    packages=find_packages(),
    install_requires=REQUIRED_PACKAGES,
    author='Grid Dynamics ML Engineer',
    author_email='griddynamics@griddynamics.com',
    url='https://griddynamics.com'
)
