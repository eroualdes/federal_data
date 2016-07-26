from setuptools import setup

setup(
  name='fd',
  version='0.1',
  py_modules=['fd'],
  install_requires=[
    'pandas',
    'requests',
  ],
  entry_points={
    'console_scripts': ['fd=fd:main']
  }
)
