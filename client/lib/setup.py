from setuptools import setup

setup(
  name='cer',
  version='0.1',
  description='Library for remote execution and file transfer',
  author='Martin Feller',
  author_email='m.feller@auckland.ac.nz',
  packages=['cer', 'cer.client', 'cer.client.util', 'cer.client.ssh', 'cer.client.job', 'cer.client.pypass'],
  install_requires=['paramiko', 'pyotp', 'python-gnupg', 'pyInstaller', 'prompt_toolkit']
)
