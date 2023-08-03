import logging
from requests import Session
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests.exceptions import RetryError, MissingSchema, HTTPError

def requests_retry(url, verify_ssl=True):
    """
    Custom function for HTTPS request functions with retry and backoff
    """
    s = requests.Session()
    
    # Retry for following HTTP response codes:
    # 404 - Not Found
    # 429 - Too Many Requests
    # 500 - Internal Server Error
    # 502 - Bad Gateway
    # 503 - Service Unavailable
    # 504 - Gateway Timeout
    retries = Retry(total=5,
                    backoff_factor=1,
                    status_forcelist=[404, 429, 500, 502, 503, 504])

    s.mount('https://', HTTPAdapter(max_retries=retries))

    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"}
        page_request = s.get(url, verify=verify_ssl, headers=headers)
    except RetryError:
        logging.warning(f'\nMax retries exceeded with url: {url}')
        return None
    except MissingSchema as errmiss:
        logging.critical(f'\nInvalid schema for {url}')
        return None
    except HTTPError as errh:
        logging.critical(f'\n{errh.args[0]}')
        return None

    return page_request