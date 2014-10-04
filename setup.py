from distutils.core import setup

setup(
    name='scrAPI Utils',
    version='0.5.2',
    author='Chris Seto',
    author_email='Chris@seto.xyz',
    packages=['scrapi.linter'],
    package_data={'scrapi.linter': ['../__init__.py']},
    url='http://www.github.com/chrisseto/scrapi',
    license='LICENSE.txt',
    description='Package to aid in consumer creation for scrAPI',
    long_description=open('README.md').read(),
)
