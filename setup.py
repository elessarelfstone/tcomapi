from setuptools import setup, find_packages


from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(name='tcomapi',
      version='0.1.12',
      description='Tool for retrieving information on tax payments by Kazakhstan companies',
      url='https://github.com/elessarelfstone/tcomapi',
      author='Dauren Sdykov',
      author_email='elessarelfstone@mail.ru',
      license='MIT',
      packages=find_packages(),
      include_package_data=True,
      python_requires='>=3.6.1',
      setup_requires=[
          'wheel',
      ],
      install_requires=[
          'aiohttp',
          'attrs>=19.3.0',
          'python-box>=3.4.5',
          'requests>=2.22.0',
          'xmltodict>=0.12.0',
          'tqdm>=4.38.0'
      ],
      extras_require={
          'dev': [
              'setuptools>=38.6.0',
              'wheel>=0.31.0',
              'twine>=1.11.0',
          ],
      },
      entry_points={
          'console_scripts': [
              'kgd=tcomapi.kgd.__main__:main',
              'sgov=tcomapi.sgov.__main__:main'
          ],
      },
      classifiers=[
          "Programming Language :: Python :: 3.6",
          "License :: OSI Approved :: MIT License",
          "Operating System :: OS Independent",
          "Development Status :: 4 - Beta",
          "Environment :: No Input/Output (Daemon)",
          "Intended Audience :: System Administrators",
          "Natural Language :: English",
          "Topic :: Internet",
      ],
      long_description=long_description,
      long_description_content_type='text/markdown',
      zip_safe=True)
