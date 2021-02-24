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


configs = {
    "app_path" : os.path.dirname(os.path.abspath(__file__)),
    "base_path" : os.path.join( os.path.dirname(os.path.abspath(__file__)), "../") ,
    "google_auth" : os.path.join( os.path.join( os.path.dirname(os.path.abspath(__file__)), "../"), "lib/google_auth.json" ) ,
    "worksheet_key": "",                                      # get this from the sheet url
    "tab_name": "Data",                                                                                   # the google sheet tab to post the data into
}


class DataPull():

    def __init__(self, configs):
        self.configs = configs
        self.gcreds = pygsheets.authorize(service_file=configs.get( "google_auth" ))
        self.data = None

    def pull_from_sheet(self, tab_name):

        sheet_access = self.gcreds.open_by_key( self.configs.get("worksheet_key") )
        # first_row = start_cell[0]
        # last_row = rows + 1
#         end_cell = (rows, cols)

        try:
            tab = sheet_access.worksheet_by_title(tab_name)
            data = tab.get_as_df(has_header=True, numerize=True, empty_value=None, include_tailing_empty=False)

            # process the dataframe here
            self.data = data
            # return data

        except pygsheets.exceptions.WorksheetNotFound:
            print(f"{tab_name} sheet not found")
            sys.exit(1)
        except Exception as err:
            print(f"{err} got when getting the sheet {tab_name}")
        finally:
            print("-" * 100)

def main():
    etl = DataPull( configs )
    etl.pull_from_sheet( "Data" )
    data = etl.data
    return data


if __name__ == "__main__":
    data = main()
    print( data )
    # print( os.listdir( configs.get("base_path")) )
    # print( configs.get("google_auth") ) #, configs.get("google_auth")) )

