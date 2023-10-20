import pandas as pd
import sys, os
from glob import glob


def combine_from_csv(csv_files=None, csv_dir=None):
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
        df_phivolcs = _df_concat_from_csv_list(glob(csv_dir + "/*.csv"))
    df_phivolcs.to_csv('combined.csv', encoding='windows-1252')
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

if __name__ == '__main__':
    # set option later with argparse
    df_comb = combine_from_csv(csv_files=None, csv_dir=sys.argv[1])