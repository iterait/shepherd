from setuptools import setup, find_packages

setup(name='shepherd',
      version='0.5.1',
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
      tests_require=[
        'pytest',
        'pytest-mock',
        'pytest-forked',
        'pytest-aiohttp',
        'minio>=4.0',
        'molotov',
      ],
      install_requires=[
        'click>=7.0',
        'simplejson>=3.16',
        'pyzmq>=18.0',
        'ruamel.yaml>=0.15',
        'requests>=2.21',
        'schematics>=2.1',
        'aiohttp>=3.5',
        'aiohttp-cors>=0.7',
        'aiobotocore>=0.10',
        'emloop>=0.2',
        'apistrap>=0.3',
      ],
      extras_require={
          'docs': ['sphinx==1.8.5', 'autoapi==1.3.1', 'sphinx-argparse',
                   'sphinx-autodoc-typehints', 'sphinx-bootstrap-theme'],
      },
      entry_points={
          'console_scripts': [
              'shepherd=shepherd.manage:run',
              'shepherd-runner=shepherd.runner.runner_entry_point:main'
          ]
      }
)
