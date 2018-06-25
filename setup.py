from setuptools import setup, find_packages

setup(name='cxworker',
      version='0.3.0',
      description='Worker',
      long_description='Works',
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
      url='https://github.com/cognexa/cxworker',
      author='Cognexa Solutions s.r.o.',
      author_email='info@cognexa.com',
      license='MIT',
      packages=['cxworker']+['.'.join(('cxworker', package)) for package in find_packages('cxworker')],
      include_package_data=True,
      zip_safe=False,
      setup_requires=['pytest-runner'],
      tests_require=['pytest'],
      install_requires=[line for line in open('requirements.txt', 'r').readlines() if not line.startswith('#')],
      entry_points={
          'console_scripts': [
              'cxworker=cxworker.manage:run',
              'cxworker-runner=cxworker.runner.runner_entry_point:run'
          ]
      }
)
