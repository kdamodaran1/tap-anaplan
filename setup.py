#!/usr/bin/env python

from setuptools import setup



setup(

    name="tap-anaplan",

    version="0.0.1",

    description="Singer.io tap for extracting Anaplan data",

    author="Kandasamy",

    url="http://github.com/singer-io/tap-anaplan",

    classifiers=["Programming Language :: Python :: 3 :: Only"],

    py_modules=["tap_anaplan"],

    install_requires=[

        'requests==2.20.0',

        'singer-python==5.6.0',

    ],

    extras_require={

        'dev': [

            'pylint',

            'ipdb',

        ]

    },

    entry_points="""

    [console_scripts]

    tap-anaplan=tap_anaplan:main

    """,

    packages=["tap_anaplan"],

    package_data = {

        "schemas": ["tap_anaplan/schemas/*.json"]

    },

    include_package_data=True,

)
