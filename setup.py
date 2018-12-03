from setuptools import setup, find_packages

setup(name='shepherd',
      version='0.3.1',
      description='Shepherd',
      long_description='Asynchronous worker',
      classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Education',
        'Intended Audience :: Science/Research',
        'Operating System :: Unix',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6'
      ],
      keywords='worker',
      url='https://github.com/iterait/shepherd',
      author=['Iterait a.s.', 'Cognexa Solutions s.r.o.'],
      author_email='info@iterait.com',
      license='MIT',
      packages=['shepherd']+['.'.join(('shepherd', package)) for package in find_packages('shepherd')],
      include_package_data=True,
      zip_safe=False,
      setup_requires=['pytest-runner'],
      tests_require=['pytest', 'pytest-mock', 'pytest-aiohttp'],
      install_requires=['click', 'aiohttp', 'simplejson', 'pyzmq', 'gevent', 'PyYaml', 'requests', 'minio',
                        'schematics', 'emloop', 'apistrap', 'aiohttp-cors', 'aiobotocore'],
      entry_points={
          'console_scripts': [
              'shepherd=shepherd.manage:run',
              'shepherd-runner=shepherd.runner.runner_entry_point:main'
          ]
      }
)
