import logging
import sys
import traceback

def exception_logging(exctype, value, tb):
    """
    Log exception by using the root logger.

    Parameters
    ----------
    exctype : type
    value : NameError
    tb : traceback
    """
    logging.critical(f'{str(exctype)}\n'
                     f'{str(value)}\n'
                     f'{str(traceback.format_tb(tb, 10))}')
    print(f'EXCEPTION TYPE: {str(exctype)}\nVALUE: {str(value)}\n'
          f'MESSAGE: {str(traceback.format_tb(tb, 10))}', 
          file=sys.stderr)