KgdTaxes
========

Tool for retrieving information on tax payments by Kazakhstan companies.

## Features

* progressbar with additional info
* retries for unreliable networks or if service temporarily is not available
* max output file size limitation


## Usage

```
$ kgd --help
usage: __main__.py [-h] [-B BACKOFF] [-w TIMEOUT] [-r RETRIES] [-f FSIZE]
                   address_port token fpath date_range


positional arguments:
  address_port          KGD API target host and port
  token                 KGD API secret token
  fpath                 input file with BINs
  date_range            date range for which we retrieve data

optional arguments:
  -h, --help            show this help message and exit
  -B BACKOFF, --backoff BACKOFF
                        delay after each failed attempt (default: 0.5)
  -w TIMEOUT, --timeout TIMEOUT
                        server connect timeout (default: 4)
  -r RETRIES, --retries RETRIES
                        retries count for one BIN (default: 4)
  -f FSIZE, --fsize FSIZE
                        size limit for output file (default: 50000000)
```

#### Usage examples

```
kgd 113.234.167.155:9910 sdfgggw5gsdg4g4g4435dg /home/user/bins.txt -w 6 --backoff 2 2018-01-01:2019-01-01 -f 500000000 
```

Note: In this example address:port and secret token are fake. To run command successfully 
you need get them from KGD government agency.

## Installation

Python 3.6.1+ required.

From PyPI:

```
pip install kgd
```

