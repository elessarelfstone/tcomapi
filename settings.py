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
TEST_DIR = os.path.join(os.path.expanduser('~'), 'test')
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

STATGOV_TIMEOUT = 1000

TELECOM_API_TOKEN_URI = os.getenv('TELECOM_API_TOKEN_URI')
TELECOM_API_HOST = os.getenv('TELECOM_API_HOST')
TELECOM_API_HEADERS = {
    "accept": "application/json"
}

TELECOM_API_TOKEN_URL = f'https://{TELECOM_API_HOST}{TELECOM_API_TOKEN_URI}'
TELECOM_API_CLIENT_ID = os.getenv('TELECOM_API_CLIENT_ID')
TELECOM_API_CLIENT_SECRET = os.getenv('TELECOM_API_CLIENT_SECRET')
TELECOM_API_USER_NAME = os.getenv('TELECOM_API_USER_NAME')
TELECOM_API_PASSWORD = os.getenv('TELECOM_API_PASSWORD')
TELECOM_API_URL_PATTERN = f'https://{TELECOM_API_HOST}/api/v1/citizen/phone-verification-state/{{}}/{{}}'


SK_USER = os.getenv('SK_USER')
SK_PASSWORD = os.getenv('SK_PASSWORD')
SK_TCOM_COMPANY_ID = os.getenv('SK_TCOM_COMPANY_ID')


INFOBIP_API_URL = 'https://9rrrjd.api.infobip.com/ccaas/1/'
INFOBIP_API_USER = os.getenv('INFOBIP_API_USER')
INFOBIP_API_PASS = os.getenv('INFOBIP_API_PASS')

