"""
Alpaca Consts, Environment Variables and Authentication
Helper
"""

import os
import spylunking.log.setup_logging as log_utils

log = log_utils.build_colorized_logger(name=__name__)

AP_ENDPOINT_API = os.getenv(
    'AP_ENDPOINT_API',
    'api.alpaca.markets')
AP_ENDPOINT_DATA = os.getenv(
    'AP_ENDPOINT_DATA',
    'data.alpaca.markets')
AP_ENDPOINT_STREAM = os.getenv(
    'AP_ENDPOINT_STREAM',
    'stream.alpaca.markets')
AP_API_KEY_ID = os.getenv(
    'AP_API_KEY_ID',
    'MISSING_AP_API_KEY_ID')
AP_SECRET_KEY = os.getenv(
    'AP_SECRET_KEY',
    'MISSING_AP_SECRET_KEY')
AP_URLS = {
    'account': (
        f'https://{AP_ENDPOINT_API}'
        '/v2/account'),
    'options': (
        f'https://{AP_ENDPOINT_DATA}'
        '/v2/options/chains'
        f'?symbol={"{}"}&expiration={"{}"}')
}

FETCH_AP_CALLS = 20000
FETCH_AP_PUTS = 20001

DATAFEED_AP_CALLS = 21000
DATAFEED_AP_PUTS = 21001

DEFAULT_FETCH_DATASETS_AP = [
    FETCH_AP_CALLS,
    FETCH_AP_PUTS
]
TIMESENSITIVE_DATASETS_AP = [
    FETCH_AP_CALLS,
    FETCH_AP_PUTS
]

ENV_FETCH_DATASETS_AP = os.getenv(
    'ENV_FETCH_DATASETS_AP',
    None)

if ENV_FETCH_DATASETS_AP:
    SPLIT_FETCH_DATASETS_AP = ENV_FETCH_DATASETS_AP.split(',')
    
DEFAULT_FETCH_DATASETS_AP = []
for d in SPLIT_FETCH_DATASETS_AP:
    if d == 'apcalls':
        DEFAULT_FETCH_DATASETS_AP.append(
        FETCH_AP_CALLS)
    elif d == 'apputs':
        DEFAULT_FETCH_DATASETS_AP.append(
        FETCH_AP_PUTS)

# end of handling custom 

ENV_FETCH_DATASETS_AP
FETCH_DATASETS_AP = DEFAULT_FETCH_DATASETS_AP

AP_OPTION_COLUMNS = [
'ask',
'ask_date',
'asksize',
'bid',
'bid_date',
'bidsize',
'date',
'exp_date',
'last',
'last_volume',
'open_interest',
'opt_type',
'strike',
'ticker',
'trade_date',
'created',
'volume'
]

AP_EPOCH_COLUMNS = [
'ask_date',
'bid_date',
'trade_date'
]
def get_ft_str_ap(
        ft_type):
    """get_ft_str_ap

    :param ft_type: enum fetch type value to return
                    as a string
    """
    if ft_type == FETCH_AP_CALLS:
        return 'apcalls'
    elif ft_type == FETCH_AP_PUTS:
        return 'apputs'
    else:
        return f'unsupported ft_type={ft_type}'
# end of get_ft_str_ap


def get_datafeed_str_ap(
        df_type):
    """get_datafeed_str_ap

    :param df_type: enum fetch type value to return
                    as a string
    """
    if df_type == DATAFEED_AP_CALLS:
        return 'apcalls'
    elif df_type == DATAFEED_AP_PUTS:
        return 'apputs'
    else:
        return f'unsupported df_type={df_type}'
# end of get_datafeed_str_ap


def get_auth_headers(
        use_api_key_id=AP_API_KEY_ID,
        use_secret_key=AP_SECRET_KEY,
        env_api_key_id=None,
        env_secret_key=None):
    """get_auth_headers

    Get connection and auth headers for Alpaca account:
    https://alpaca.markets/docs/api-documentation/

    :param use_api_key_id: optional - API key ID
        instead of the default ``AP_API_KEY_ID``
    :param use_secret_key: optional - secret key
        instead of the default ``AP_SECRET_KEY``
    :param env_api_key_id: optional - env key to use
        instead of the default ``AP_API_KEY_ID``
    :param env_secret_key: optional - env key to use
        instead of the default ``AP_SECRET_KEY``
    """
    api_key_id = AP_API_KEY_ID if not env_api_key_id else os.getenv(env_api_key_id, AP_API_KEY_ID)
    secret_key = AP_SECRET_KEY if not env_secret_key else os.getenv(env_secret_key, AP_SECRET_KEY)

    headers = {
        'Accept': 'application/json',
        'APA-API-KEY-ID': api_key_id,
        'APA-API-SECRET-KEY': secret_key
    }
    return headers
# end of get_auth_headers

