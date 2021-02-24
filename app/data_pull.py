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
}


class DataPull():

    def __init__(self, google_auth):
        self.google_auth = google_auth
        self.gcreds = pygsheets.authorize(service_file=self.google_auth)
        self.data = None

    def pull_data(self, tab_name):

        sheet_access = self.gcreds.open_by_key( self.configs.get("worksheet_key") )
        first_row = start_cell[0]
        last_row = rows + 1
#         end_cell = (rows, cols)

        try:
            tab = sheet_access.worksheet_by_title(tab_name)
            data = tab.get_As_Df(has_header=True, include_tailing_empty=False)
            self.data = data
            # return data

        except Exception as err:
            raise

def main():
    etl = DataPull( configs.get("google_auth") )
    etl.pull_data( "Data" )
    data = etl.data


if __name__ == "__main__":
    data = main()
    print( data )

