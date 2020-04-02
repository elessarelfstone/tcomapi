import os
from tempfile import gettempdir

from dotenv import load_dotenv

load_dotenv()

KGD_API_TOKEN = os.getenv('KGD_API_TOKEN')
DGOV_API_KEY = os.getenv('DGOV_API_KEY')


# TMP_DIR = gettempdir()
TMP_DIR = os.path.join(os.path.expanduser('~'), 'data')
EVENTS_DB_PATH = os.path.join(os.path.expanduser('~'), 'data', 'events.db')

WORK_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(WORK_DIR, 'tasks', 'config')


FTP_PATH = '/external_sources/http_test2/'
FTP_HOST = '10.8.36.51'
FTP_USER = 'ftpuser'
FTP_PASS = 'ftpuser'
