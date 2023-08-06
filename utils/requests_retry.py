import logging
from requests import Session
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests.exceptions import RetryError, MissingSchema, HTTPError
from fake_useragent import UserAgent

def requests_retry(url, verify_ssl=True):
    """
    Custom function for HTTPS request functions with retry and backoff
    """
    s = Session()
    
    # Except for following HTTP response codes:
    # 404 - Not Found
    # 429 - Too Many Requests
    # 500 - Internal Server Error
    # 502 - Bad Gateway
    # 503 - Service Unavailable
    # 504 - Gateway Timeout
    retries = Retry(total=5,
                    backoff_factor=1,
                    status_forcelist=[429, 500, 502, 503, 504])

    s.mount('https://', HTTPAdapter(max_retries=retries))

    try:
        ua = UserAgent()
        headers = {"User-Agent": ua.random}
        page_request = s.get(url, verify=verify_ssl, headers=headers, timeout=9)
        page_request.raise_for_status()
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