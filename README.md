Kgd
========

Tool for retrieving information on tax payments by Kazakhstan companies.

## Features

* progressbar with additional info
* max output file size limitation


## Usage

```
$ kgd --help
usage: kgd [-h] [-t TIMEOUT] [-f FSIZE] token fpath date_range


positional arguments:
  token                 KGD API secret token
  fpath                 input file with BINs
  date_range            date range for which we retrieve data

optional arguments:
  -h, --help            show this help message and exit
  -w TIMEOUT, --timeout TIMEOUT
                        server connect and failure timeout (default: 3.0)
  -f FSIZE, --fsize FSIZE
                        size limit for output file (default: 50000000)
```

#### Usage examples

```
kgd sdfgggw5gsdg4g4g4435dg /home/user/bins.txt -t 3  2018-01-01:2019-01-01 -f 500000000 
```

Note: In this example secret token is fake. To run command successfully 
you need get them from KGD government agency.

## Installation

Python 3.6.1+ required.

From PyPI:

```
pip install kgd
```

