#!/usr/bin/env python
from os import chdir
from glob import glob
from setuptools import setup
from setuptools.command.install import install

chdir('pyunlocode')
package_data = glob('csv/*.csv')
chdir('..')
package_data.append('unlocode.db')

setup(name='pyunlocode',
      version='0.0.1',
      description='Librarys to check zones configuered on a server are working',
      author='Falldog and John Bond',
      author_email='pypi@johnbond.org',
      url='https://github.com/b4ldr/pyunlocode',
      license='Artistic-2.0',
      packages=['pyunlocode'],
      keywords='',
      install_requires=[
          'math',
          'sqlite3',
          'csv',
          ],
      package_data={'pyunlocode' : package_data }
     )
