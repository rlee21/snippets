from setuptools import setup

setup(
    name='wdep',
    description='CLI tool to package/deploy/run oozie workflows',
    version='0.1',
    packages=['wdep'],
    test_suite='tests',
    tests_require=[
        'mock',
    ],
    install_requires=[
        'fabric',
    ],
    entry_points={
        'console_scripts': [
            'wdep = wdep.cli:main',
        ],
    }
)

