from setuptools import setup

setup(name='wildcard_pyevents',
      version='0.1',
      description='The funniest joke in the world',
      url='http://github.com/trywildcard/wildcard_pyevents',
      author='Flying Circus',
      author_email='eric@trywildcard.com',
      license='MIT',
      packages=['wildcard_pyevents'],
      install_requires=['redis', 'influxdb'],
      zip_safe=False)