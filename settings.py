import os
from tempfile import gettempdir

from dotenv import load_dotenv

load_dotenv()

PRODUCTION_HOST = os.getenv('PRODUCTION_HOST')
PRODUCTION_USER = os.getenv('PRODUCTION_USER')
PRODUCTION_PASS = os.getenv('PRODUCTION_PASS')


KGD_API_TOKEN = os.getenv('KGD_API_TOKEN')
DGOV_API_KEY = os.getenv('DGOV_API_KEY')
GOSZAKUP_TOKEN = os.getenv('GOSZAKUP_TOKEN')

# TMP_DIR = gettempdir()
TMP_DIR = os.path.join(os.path.expanduser('~'), 'data')
BIGDATA_TMP_DIR = os.path.join(os.path.expanduser('~'), 'bigdata')
ARCH_DIR = os.path.join(os.path.expanduser('~'), 'arch')

EVENTS_DB_PATH = os.path.join(os.path.expanduser('~'), 'data', 'events.db')

WORK_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(WORK_DIR, 'tasks', 'config')


FTP_PATH = os.getenv('FTP_PATH')
FTP_HOST = os.getenv('FTP_HOST')
FTP_USER = os.getenv('FTP_USER')
FTP_PASS = os.getenv('FTP_PASS')

CH_MEDIATION_ARCH_HOST = os.getenv('CH_MEDIATION_ARCH_HOST')
CH_MEDIATION_ARCH_PORT = os.getenv('CH_MEDIATION_ARCH_PORT')
CH_MEDIATION_ARCH_USER = os.getenv('CH_MEDIATION_ARCH_USER')
CH_MEDIATION_ARCH_PASS = os.getenv('CH_MEDIATION_ARCH_PASS')


FTP_IN_PATH = os.getenv('FTP_IN_PATH')
