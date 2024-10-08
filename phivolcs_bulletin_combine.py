import pandas as pd
import sys, os
from glob import glob


def combine_from_csv(csv_files=None, csv_dir=None, hmtk_output=True):
    """
    Returns combined dataframe from csv file/s or directory of csv file/s
    """
    df_phivolcs = pd.DataFrame()
    if csv_files is None and csv_dir is None:
        raise ValueError("Please input either csv file/s or csv directory name")
    
    elif csv_dir is None: # scalar or list of csv file/s is specified
        df_phivolcs = _df_concat_from_csv_list(list(csv_files))

    elif csv_files is None: # directory is specified
        if not os.path.isdir(csv_dir):
            raise FileNotFoundError("Directory does not exist")
        
        os.chdir(csv_dir)
        all_csv = glob(csv_dir + "/*.csv")
        monthly_data = [f for f in all_csv 
                        if is_valid_year(os.path.basename(f).split('_')[0])]
        
        df_phivolcs = _df_concat_from_csv_list(monthly_data)

    df_phivolcs.sort_values(by=['datetime'], inplace=True)    
    df_phivolcs.to_csv('combined.csv', encoding='windows-1252')
    # add eventID column for ISC-like counter
    df_phivolcs.insert(0, 'eventID', 
                       range(61200000, 61200000 + len(df_phivolcs)))

    df_filtered = filter_df_phivolcs(df_phivolcs)
    df_filtered.to_csv('combined_minM4pt5.csv', encoding='windows-1252', 
                       index=False)

    if hmtk_output:
        df_dt = pd.to_datetime(df_filtered.pop('datetime'))
        df_filtered.insert(1, 'year', df_dt.dt.year)
        df_filtered.insert(2, 'month', df_dt.dt.month)
        df_filtered.insert(3, 'day', df_dt.dt.day)
        df_filtered.insert(4, 'hour', df_dt.dt.hour)
        df_filtered.insert(5, 'minute', df_dt.dt.minute)
        df_filtered.insert(6, 'second', df_dt.dt.second)

        df_filtered.to_csv('combined_hmtk_minM4pt5.csv', 
                           encoding='windows-1252', index=False)

    return df_phivolcs
    

def filter_df_phivolcs(df: pd.DataFrame):
    """
    Returns filtered/processed dataframe using the following logic:

    - Filtered out events less than M4.5
    - 'unspecified' event types set to TECTONIC for events greater than M4.5
    - mag_type fields set to 'Ms' for 'unspecified' event_type events less
        than M5.5 (but greater than M4.5)
    - mag_type fields set to 'Mw' for 'unspecified' event_type events greater
        than or equal to M5.5
    """
    # Pre-process
    df['event_type'] = df['event_type'].str.strip()
    df['event_type'] = df['event_type'].str.replace('VOLCANO|VOLANO', 
                                                    'VOLCANIC', regex=True)
    # Get all data with magnitudes of at least 4.5
    df = df[df['mag'] >= 4.5]
    # Apply criteria on setting Ms as assumed magnitude type for unspec events
    df.loc[(df['event_type'] == 'unspecified') & 
           (df['mag'] < 5.5), 'mag_type'] = 'Ms'
    # Apply criteria on setting Mw as assumed magnitude type for unspec events
    df.loc[(df['event_type'] == 'unspecified') 
           & (df['mag'] >= 5.5), 'mag_type'] = 'Mw'
    # Apply criteria on setting TECTONIC as assumed event type for unspec events
    df.loc[df['event_type'] == 'unspecified', 'event_type'] = 'TECTONIC'

    return df


def _df_concat_from_csv_list(csv_list: list):
    df_comb = pd.DataFrame()
    for f in csv_list:
        df = pd.read_csv(f, header=0, index_col=0, encoding='windows-1252')
        df_comb = pd.concat([df_comb, df], ignore_index=True)
    
    return df_comb


def is_valid_year(year_str, min_year=2014, max_year=2200):
    try:
        year = int(year_str)
        if min_year <= year <= max_year:
            return True
        else:
            return False
    except ValueError:
        return False


if __name__ == '__main__':
    # set option later with argparse
    df_comb = combine_from_csv(csv_files=None, csv_dir=sys.argv[1])