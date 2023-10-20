import pandas as pd
import sys
from phivolcs_bulletin import _convert_pst_to_utc
from numpy import nan


def get_xlsx(fname, sheets=None):
    """
    Return dataframe from xlsx file
    """
    heads = ['datetime', 'lat', 'lon', 'depth_km', 'mag', 'location']

    df_dict = pd.read_excel(fname, names=heads, engine='openpyxl', 
                            sheet_name=sheets,
                            converters={0: _convert_pst_to_utc}, 
                            na_values = '-')
    
    df_phivolcs = pd.DataFrame()
    for sheet in df_dict:
        df_phivolcs = df_dict[sheet]
        df_phivolcs = df_phivolcs.assign(mag_type='unspecified', 
                                         event_type='unspecified')
        year = df_phivolcs.at[0, 'datetime'].year
        # set output option later with argparse
        df_phivolcs.to_csv(f"{year}_{sheet}.csv", encoding='windows-1252') 
        
    return df_phivolcs


if __name__ == '__main__':
    df_phv = get_xlsx(sys.argv[1])