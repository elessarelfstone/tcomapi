import os
from tempfile import gettempdir

from dotenv import load_dotenv

load_dotenv()

KGD_API_TOKEN = os.getenv('KGD_API_TOKEN')
DGOV_API_KEY = os.getenv('DGOV_API_KEY')


# TMP_DIR = gettempdir()
TMP_DIR = os.path.join(os.path.expanduser('~'), 'data')
ARCH_DIR = os.path.join(os.path.expanduser('~'), 'arch')

EVENTS_DB_PATH = os.path.join(os.path.expanduser('~'), 'data', 'events.db')

WORK_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(WORK_DIR, 'tasks', 'config')


# FTP_PATH = '/external_sources/http_test2/'
# FTP_HOST = '10.8.36.51'
# FTP_USER = 'ftpuser'
# FTP_PASS = 'ftpuser'

FTP_PATH = os.getenv('FTP_PATH')
FTP_HOST = os.getenv('FTP_HOST')
FTP_USER = os.getenv('FTP_USER')
FTP_PASS = os.getenv('FTP_PASS')

TEMP = ''