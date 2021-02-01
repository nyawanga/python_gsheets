import sys
import os
import re
sys.path.append("../")

import pandas as pd
import pygsheets
from lib.db_connector import get_connection                           # from the db connection module
from oauth2client.service_account import ServiceAccountCredentials

import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json

from decimal import Decimal
from collections import OrderedDict

# https://github.com/nithinmurali/pygsheets/issues/124
# https://pygsheets.readthedocs.io/en/stable/_modules/pygsheets/worksheet.html

app_path = os.path.dirname(os.path.abspath(__file__))
base_path = os.path.join(app_path, "../")
auth_file = os.path.join(base_path, "files/google_auth.json")
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive',
         ]

worksheets = {
     "weekly_reporting": {
         "worksheet_key": "",      # get this from the sheet url
         "tab_name": "data",       # the google sheet tab to post the data into
     },

    # "monthly_reporting": {
    #     "worksheet_key": "",
    #     "tab_name": "data",
    # },
}


class DecimalEncoder(json.JSONEncoder):
    def _iterencode(self, o, markers=None):
        if isinstance(o, Decimal):
            # wanted a simple yield str(o) in the next line,
            # but that would mean a yield on the line with super(...),
            # which wouldn't work (see my comment below), so...
            return (str(o) for o in [o])
        return super(DecimalEncoder, self)._iterencode(o, markers)


def get_credentials(auth_file=auth_file):
    # credentials = ServiceAccountCredentials.from_json_keyfile_name( self.auth_file, self.scope )
    gcreds = pygsheets.authorize(service_file=auth_file)
    return gcreds


def get_data(sql_script):
    conn = get_connection()
    cursor = conn.cursor()
    data = []
    cursor.execute("USE table_name;")
    with open(sql_script, "r") as script:
        content = script.read()
        cursor.execute(content)

        results = [OrderedDict(row) for row in cursor.fetchall()]
        data = results
        cols_order = results[0].keys()
    #     for row in results:  # cursor.fetchall():
    #         # print(row)
    #         data.append(list(row.values()))
    # print(data[:3])
    return data, cols_order


def clean_data(data):
    new_data = []
    row_len = []
    rows = len(data)
    # print(rows)
    for idx, row in enumerate(data):
        # if idx < 10:  # len(data) + 1:
        new_row = OrderedDict()
        num_rows = len(row)
        row_len.append(num_rows)
        for key, value in row.items():
            if isinstance(value, Decimal):
                new_row[key] = float(value)
            elif value is None:
                new_row[key] = ''
            else:
                new_row[key] = str(value)
        # print(new_row.keys())
        new_data.append(new_row)                # append the cleaned row

    cols = max(row_len)
    return new_data, rows, cols


def write_to_sheets(data, rows, cols, cols_order, start_cell=(2, 1), tab_name="Data", as_df=False):
    """
    Function to post the data to a google sheets.
        
    Parameters:
        data       - list of list
        rows, cols - integer number of cols and rows in the data
        cols_order - order of column headers
        start_cell - the cell to start posting data on the google sheet
        end_cell   - the cell to end the data
        tab_name   - the tab to post the data on the google sheet
        as_df      - whether to post as dataframe
        
    Returns:
        None
    """

    credentials = get_credentials()
    sheet_access = credentials.open_by_key(worksheet_key)
    first_row = start_cell[0]
    last_row = rows + 1
    end_cell = (rows, cols)

    try:
        tab = sheet_access.worksheet_by_title(tab_name)
        print(f"deleting data from range {start_cell} ... ")
        print("-" * 100)
        # tab.clear('A{0}:H{1}'.format(first_row, last_row))
        tab.clear(start=start_cell, end=end_cell)                              # set end_cell to None to clear the whole sheet
        col_names = tab.get_values(start=(1, 1), end=(1, cols))[0]
        sort_data = [[row[item] for item in col_names] for row in data]
#         sort_data = ['' for item in sort_data if item is None]
        # print(col_names)
        print("updating data now ...")
        if as_df:
            data = pd.DataFrame(data, columns=cols_order)
            data = data[col_names]
            # print(data.head(2))
            tab.set_dataframe(data, start_cell, fit=True, copy_head=False, escape_formulae=True)
        else:
            # print(sort_data[:2])
            # tab.update_values(crange='A{0}:H{1}'.format(first_row, last_row), values=data, extend=True)
            tab.update_values(crange=start_cell, values=sort_data, extend=True)
    except pygsheets.exceptions.WorksheetNotFound:
        print(f"{tab_name} sheet not found")
        sys.exit(1)
    except Exception as err:
        print(f"{err} got when attempting to get the sheet {tab_name}")
    finally:
        print("-" * 100)


# incase you want to save output to csv file
def write_to_csv(self, data, outfile="data.csv"):
    """
    A function to write data to a file

    Parameters:
        data      - list of list
        outfile   - the default name of file to output data to.

    Returns:
        None
    """

    with open(outfile, "w", encoding='utf-8') as f:
        import csv
        csv_writer = csv.writer(f, dialect='excel', lineterminator='\n', quoting=csv.QUOTE_NONNUMERIC)
        print( f"writting to file {outfile}")
        [ csv_writer.writerow(row) for row in data ]
        print("done writting file")


for worksheet in worksheets:
    sql_script = os.path.join(base_path, "files/{}.sql".format(worksheet))
    worksheet_key = worksheets.get(worksheet).get("worksheet_key")
    tab_name = worksheets.get(worksheet).get("tab_name")

    # print(f"{sql_script} : {worksheet_key} : {tab_name}")
    # tab_name = 'data'
    data, cols_order = get_data(sql_script=sql_script)
    data, rows, cols = clean_data(data)
    print(f"worksheet: {worksheet}, rows :{rows}, cols:{cols}")
    print("-" * 100)
    # # time.sleep(10)
    # # print(data)
    # # data = json.dumps(new_data)
    # # write_to_csv(data)
    write_to_sheets(
        data,
        rows=rows,
        cols=cols,
        cols_order=cols_order,
        start_cell=(2, 1),
        tab_name=tab_name,
        as_df=False)
    time.sleep(30)
