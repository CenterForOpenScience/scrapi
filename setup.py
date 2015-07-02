from setuptools import setup, find_packages

setup(
    name='scrapi',
    version="DEV",
    package_dir={'scrapi': 'scrapi'},
    packages=find_packages(),
    platforms=["any"],
    classifiers=[
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
    ],
)
