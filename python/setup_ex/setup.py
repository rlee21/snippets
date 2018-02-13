import os
from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))
about = {}
with open(os.path.join(here, 'setup_ex', '__version__.py')) as fh:
    exec(fh.read(), about)

setup(
    name='setup_ex',
    description='example custom python package',
    version = about['version'],
    packages=['setup_ex'],
    test_suite='tests',
    tests_require=[
        'mock',
    ],
    install_requires=[
        'requests',
    ],
    entry_points={ 'console_scripts': [ 'se = setup_ex.cli:main', ] },
)

