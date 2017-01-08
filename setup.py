from setuptools import setup

setup(
    name='fd',
    version='0.1',
    py_modules=['fd'],
    package_data={'data': ['*.csv', ], },
    install_requires=[
        'pandas',
        'requests',
        'pytest',
    ],
    entry_points={
        'console_scripts': ['fd=fd:main'],
    },
)
