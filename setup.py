from setuptools import setup, find_packages

tests_require = [
    'pytest',
    'pytest-mock',
    'pytest-aiohttp',
    'molotov',
]

setup(name='shepherd',
      version='0.5.2',
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
        'Programming Language :: Python :: 3.7'
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
      tests_require=tests_require,
      install_requires=[
        'click>=7.0',
        'simplejson>=3.16',
        'pyzmq>=18.0',
        'ruamel.yaml>=0.15',
        'requests>=2.21',
        'schematics>=2.1',
        'aiohttp>=3.5',
        'aiohttp-cors>=0.7',
        'emloop>=0.2',
        'apistrap>=0.6',
        'minio>=4.0',
        'urllib3==1.24.1'
      ],
      extras_require={
          'docs': ['sphinx>=2.0', 'autoapi>=1.4', 'sphinx-argparse',
                   'sphinx-autodoc-typehints', 'sphinx-bootstrap-theme'],
          'tests': tests_require,
      },
      entry_points={
          'console_scripts': [
              'shepherd=shepherd.manage:run',
              'shepherd-runner=shepherd.runner.runner_entry_point:main'
          ]
      }
)
