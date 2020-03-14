Sgov
========

Tool for retrieving information about Kazakhstan juridical persons.

## Features

* progressbar with additional info
* max output file size limitation
* boosting with asynchronous processing


## Usage

```
$ sgov --help
usage: sgov [-h] [-rl RATELIMIT] [-sl SEMLIMIT] [-f FSIZE] fpath

Tool for retrieving information on tax payments by Kazakhstan companies

positional arguments:
  fpath                 input file with BINs

optional arguments:
  -h, --help            show this help message and exit
  -rl RATELIMIT, --ratelimit RATELIMIT
                        ratelimit (default: 10)
  -sl SEMLIMIT, --semlimit SEMLIMIT
                        semaphore limit (default: 20)
  -f FSIZE, --fsize FSIZE
                        size bytes per output file (default: 100000000)
```

#### Usage examples

```
sgov /home/user/bins.txt -f 500000000 
```

Note: In this example secret token is fake. To run command successfully 
you need get them from KGD government agency.

## Installation

Python 3.6.1+ required.

From PyPI:

```
pip install kgd
```

