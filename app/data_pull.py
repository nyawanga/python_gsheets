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
