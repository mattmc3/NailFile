from distutils.core import setup

setup(
    name='NailFile',
    version='0.0.1',
    author='Matt McElheny',
    author_email='mattmc3@gmail.com',
    packages=['nailfile', 'nailfile.test'],
    scripts=[],
    url='http://pypi.python.org/pypi/NailFile/',
    license='LICENSE.txt',
    description='.',
    long_description=open('README.txt').read(),
    install_requires=[
    ],
)