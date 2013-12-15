#!/usr/bin/env python
#:coding=utf-8:

from setuptools import setup, find_packages
 
setup (
    name='redis-mock',
    version='0.1',
    description='A mock redis client.',
    author='Ian Lewis',
    author_email='ianmlewis@gmail.com',
    url='https://github.com/IanMLewis/redis-mock/',
    classifiers=[
      'Development Status :: 3 - Alpha',
      'Intended Audience :: Developers',
      'License :: OSI Approved :: BSD License',
      'Programming Language :: Python',
      'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    packages=find_packages(),
    install_requires=[
        'redis>=2.4.8',
    ],
    test_suite='tests',
)
