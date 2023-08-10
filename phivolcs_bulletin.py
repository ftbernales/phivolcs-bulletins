import sys, logging
import pandas as pd
import pytz
from numpy import nan
from urllib3 import disable_warnings, exceptions
from bs4 import BeautifulSoup
from dateutil.parser import parse
from urllib.parse import urljoin, urlparse
from utils.requests_retry import requests_retry
from utils.exception_logging import exception_logging
from pathlib import Path
from tqdm import tqdm
from time import sleep
from charset_normalizer import detect
import argparse

disable_warnings(exceptions.InsecureRequestWarning)
sys.excepthook = exception_logging

def get_page(page_url):
    """
    Return dataframe from web page (only for format of May 2018 onwards)
    """
    url_path = urlparse(page_url).path
    output_name = Path(url_path).stem
    logging.basicConfig(filename=f'logs/{output_name}.log', filemode='w',
        level=logging.INFO, format='%(levelname)s (%(asctime)s): %(message)s')
    # Initialize PHIVOLCS page headers and output df headers
    phivolcs_heads = ['Date - Time', 'Latitude', 'Longitude', 'Depth', 'Mag',
                      'Location']
    heads = ['datetime', 'lat', 'lon', 'depth_km', 'mag', 'location']
    # Send request and get content of bulletin web page
    page_request = requests_retry(page_url, verify_ssl=False)
    try:
        soup = BeautifulSoup(page_request.content, 'html5lib', 
                        from_encoding=detect(page_request.content)['encoding'])
    except AttributeError:
        raise IOError(f"No content found in {page_url}")
    phivolcs_tables = soup.find_all('table', attrs={'class': 'MsoNormalTable'})
    # Get html table with most rows, which corresponds to the bulletin table
    table_list = [pd.read_html(str(t), header=0)[0] for t in phivolcs_tables]

    table_list_lengths = [len(df.index) for df in table_list]
    bull_idx = table_list_lengths.index(max(table_list_lengths))
    df_phivolcs = table_list[bull_idx]
    # Get href link/s in each table cell 
    event_link_dict = {}
    for i, tr in enumerate(phivolcs_tables[bull_idx].tbody.find_all('tr')):
        for td in tr.find_all('td'):
            a = td.find_all('a')
            if len(a) > 0:
                event_link_dict[i] = a[-1].get('href') # Get last href in <a>

    # Modifying the dataframe to remove blank cells, check headers, 
    # and then add Mag and Event Types 
    df_phivolcs.replace("", nan, inplace=True)
    df_phivolcs.dropna(inplace=True, thresh=3)
    df_phivolcs.reset_index(drop=True, inplace=True)
    if not any(list(map(lambda col,head: col in head, 
                    phivolcs_heads, list(df_phivolcs.columns)))):
        df_phivolcs = pd.concat([df_phivolcs.columns.to_frame().T, df_phivolcs], 
                                ignore_index=True)    
    df_phivolcs.columns = heads

    df_phivolcs = df_phivolcs.assign(mag_type=None, event_type=None,
                                     event_links=list(event_link_dict.values()))
    # Fixing dataframe cells that should be float-type values    
    num_cols = heads[1:-1]
    df_phivolcs[num_cols] = df_phivolcs[num_cols].apply(
        pd.to_numeric, errors='coerce')
    # Loop through entire dataframe
    for i, date_row in enumerate(tqdm(df_phivolcs.iloc[:,0], 
                                 desc='Reading event info: ')):
        # Fix double spaces introduced by span tags in Location column
        location = df_phivolcs.at[i,'location']
        location = location.replace('  ', ' ')
        df_phivolcs.at[i,'location'] = location
        # Assemble absolute URL from relative URL in Event Links column
        event_rel_url = df_phivolcs.at[i,'event_links'].replace('\\', '/')
        event_abs_url = urljoin(page_url, event_rel_url)
        # Get dataframe from Event Info page
        df_event_info = _get_event_info(event_abs_url)
        if df_event_info is None:
            df_phivolcs.at[i,'event_type'] = df_phivolcs.at[i,'mag_type'] = \
                'unspecified'
            # Converting datetime values (w/o sec part) from local time to UTC
            df_phivolcs.at[i,'datetime'] = _convert_pst_to_utc(
                date_row.replace(u'\ufeff', '')) # clear BOM
        else:
            df_phivolcs.at[i,'event_type'] = df_event_info.at[1,'Origin']
            df_phivolcs.at[i,'mag_type'] = \
                df_event_info.at[1,'MagType & Mag'].split()[0]
            # Converting datetime values from local time to UTC
            df_phivolcs.at[i,'datetime'] = _convert_pst_to_utc(
                df_event_info.at[1,'Date/Time (PST)'])

        sleep(0.02) # time delay for tqdm progress bar
    
    df_phivolcs.drop(columns='event_links', inplace=True)
    # set option later with argparse
    df_phivolcs.to_csv(output_name + '.csv', encoding='windows-1252') 
    return df_phivolcs


def _get_event_info(event_link):
    """
    Returns dataframe of specific earthquake event information
    """
    event_page_request = requests_retry(event_link, verify_ssl=False)
    if event_page_request is None:
        return None
    
    event_soup = BeautifulSoup(event_page_request.text, 'html.parser')
    bulletin_table = event_soup.find_all('table', 
                                           attrs={'class': 'MsoNormalTable'})    
    # Table 1 = master table; Table 2 = event info; 
    # Table 3 = reported intensities; Table 4 = expected damage/aftershocks
    try:
        event_info_table = bulletin_table[1]
    except IndexError:
        logging.warning(f'\nCannot output event info from {event_link}')
        return None
    
    df_event_info = pd.read_html(str(event_info_table))[0]
    df_event_info = df_event_info.T # Transpose dataframe
    # Set header
    df_event_info = df_event_info[1:]
    df_event_info.columns = ['Date/Time (PST)', 'Location', 'Depth (km)',
                             'Origin', 'MagType & Mag']
    
    return df_event_info


def _convert_pst_to_utc(pst_datetime):
    """
    Convert datetime in Philippine Standard Time to UTC
    """
    # PHIVOLCS date/time format; example: 01 Apr 2000 - 12:00:00 AM
    pst_tz = pytz.timezone('Asia/Manila')
    pivs_datetime = parse(pst_datetime)
    pst_datetime = pst_tz.localize(pivs_datetime)
    utc_datetime = pst_datetime.astimezone(pytz.UTC)
    return utc_datetime


if __name__ == '__main__':
    df_phv = get_page(sys.argv[1])