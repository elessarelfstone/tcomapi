import numpy as np
import pandas as pd

import attr

from tcomapi.common.utils import save_csvrows


def parse(fpath, wrapper, sheets=None,
          skiprows=None, usecols=None):
    """ Return list of records parsed from excel file """
    data = pd.DataFrame()

    # get list of sheets of excel file
    xl_df = pd.ExcelFile(fpath)
    xl_sheets = xl_df.sheet_names

    _sheets = sheets

    if not sheets:
        _sheets = [i for i, _ in enumerate(range(len(xl_sheets)))]

    # init skiprows
    _skiprows = [1 for x in range(len(_sheets))]

    if skiprows:
        _skiprows[0] = skiprows

    # parse only given sheets
    for i, sh in enumerate(_sheets):

        if sh <= len(xl_sheets)-1:
            df = pd.read_excel(fpath,
                               sheet_name=xl_sheets[sh],
                               skiprows=_skiprows[i],
                               usecols=usecols,
                               index_col=None,
                               dtype=str,
                               header=None)
            # collect data in one dataframe
            data = data.append(df, ignore_index=True)

    # convert Excel's empty cells to empty string
    data = data.replace(np.nan, '', regex=True)
    data.dropna(inplace=True)

    # wrap in specific structure
    return [wrapper(*x) for x in data.values]


def parse_to_csv(xl_fpath, csv_fpath, wrapper, sheets=None,
                 skiprows=None, usecols=None):
    """ Save records parsed from excel file to csv """
    data = pd.DataFrame()

    # get list of sheets of excel file
    xl_df = pd.ExcelFile(xl_fpath)
    xl_sheets = xl_df.sheet_names

    _sheets = sheets

    if not sheets:
        _sheets = [i for i, _ in enumerate(range(len(xl_sheets)))]

    # init skiprows
    _skiprows = [1 for x in range(len(_sheets))]

    if skiprows:
        _skiprows[0] = skiprows

    count = 0
    # parse only given sheets
    for i, sh in enumerate(_sheets):

        if sh <= len(xl_sheets) - 1:
            df = pd.read_excel(xl_fpath,
                               sheet_name=xl_sheets[sh],
                               skiprows=_skiprows[i],
                               usecols=usecols,
                               index_col=None,
                               dtype=str,
                               header=None)
            # collect data in one dataframe
            # data = data.append(df, ignore_index=True)

            # convert Excel's empty cells to empty string
            data = df.replace(np.nan, '', regex=True)
            data.dropna(inplace=True)
            rows = [wrapper(*x) for x in data.values]
            save_csvrows(csv_fpath, [attr.astuple(r) for r in rows])
            count += len(rows)

    return count
