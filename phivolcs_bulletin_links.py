import sys, argparse
import pandas as pd


def get_links_from_xlsx(*args, output=None) -> list:
    """
    Gets url from Excel file/s
    """
    links_list = []
    for path in args:
        df = pd.read_excel(path, sheet_name=None, engine='openpyxl', nrows=1)
        for keys in df:
            links_list.append(df[keys].columns.values[0])

    if output is not None:
        with open(output, 'w') as f:
            print(*links_list, sep='\n', file=f)
    else:
        print(links_list, sep='\n')
    
    return links_list


if __name__ == '__main__':
    get_links_from_xlsx(*sys.argv[1:])
    # Set later with argparse
    # get_links_from_xlsx(
    # r'C:\Users\AMH-L91\OneDrive - AMH Philippines, Inc\PHIVOLCS-EQ-2022.xlsx', 
    # output='PHIVOLCS-EQ-2022.txt')