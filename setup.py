#!/usr/bin/env python

from os.path import exists
from setuptools import setup


setup(name='gpu_streamz',
      version='0.0.2',
      description='Live GPU monitoring with streamz',
      url='http://github.com/EthanRosenthal/gpu-streamz/',
      maintainer='Ethan Rosenthal',
      maintainer_email='ethanrosenthal@gmail.com',
      license='MIT',
      long_description=(open('README.md').read() if exists('README.md')
                        else ''),
      zip_safe=False)
