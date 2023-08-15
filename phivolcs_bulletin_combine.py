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
    

def _df_concat_from_csv_list(csv_list: list):
    df_comb = pd.DataFrame()
    for f in csv_list:
        df = pd.read_csv(f, header=0, index_col=0, encoding='windows-1252')
        df_comb = pd.concat([df_comb, df], ignore_index=True)

    return df_comb

if __name__ == '__main__':
    # set option later with argparse
    df_comb = combine_from_csv(csv_files=None, csv_dir=sys.argv[1])