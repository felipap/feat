# https://stackoverflow.com/questions/15746675

from setuptools import setup, find_packages

setup(
    name='feat',
    version='0.0.1',
    description='',
    license='UNLICENSED',

    packages=find_packages(exclude=['tests*']),
    include_package_data=True,

    author='Felipe Pires',
    author_email='pires.a.felipe@gmail.com',
    keywords=[],
    url='https://github.com/felipap/feat'
)
