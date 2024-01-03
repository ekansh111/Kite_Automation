from calendar import THURSDAY
import logging
from os import abort
import string
from kiteconnect import KiteConnect
from datetime import datetime,timedelta
from datetime import date
from Set_Gtt_Exit import Set_Gtt
from inputimeout import inputimeout,TimeoutOccurred
from dateutil.relativedelta import TH,WE, relativedelta
import time
import csv
import pandas as pd

from kiteconnect import KiteConnect
from datetime import datetime,timedelta
from datetime import date
from Set_Gtt_Exit import Set_Gtt
from inputimeout import inputimeout,TimeoutOccurred
from dateutil.relativedelta import TH, relativedelta
import time
import calendar
from Directories import *

import logging
from kiteconnect import KiteConnect


with open(KiteEkanshLogin,'r') as a:
        content = a.readlines()
        a.close()
api_key = content[2].strip('\n')
kite = KiteConnect(api_key=api_key)

with open(WriteAllContractDet,'w',newline='') as csvfile: 
    # creating a csv writer object 
    csvwriter = csv.writer(csvfile) 
    
    k = kite.instruments(exchange= 'NFO')
    # Write header row
    header = ['instrument_token', 'exchange_token', 'tradingsymbol', 'name', 'last_price', 'expiry', 'strike', 'tick_size', 'lot_size', 'instrument_type', 'segment', 'exchange']
    csvwriter.writerow(header)

        # Write each record on a new line
    for record in k:
        # Extract values from the record dictionary
        row_values = [
                record['instrument_token'],
                record['exchange_token'],
                record['tradingsymbol'],
                record['name'],
                record['last_price'],
                record['expiry'],
                record['strike'],
                record['tick_size'],
                record['lot_size'],
                record['instrument_type'],
                record['segment'],
                record['exchange']
        ]
        
        csvwriter.writerow(row_values)

csvfile.close()
    #csvwriter.writerow(k) 
        
    # writing the data rows 
    #csvwriter.writerows(rows)
    
    
    #content = b.readlines()
    #b.close()
    #print()
    