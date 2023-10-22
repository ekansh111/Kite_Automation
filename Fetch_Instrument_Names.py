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

import logging
from kiteconnect import KiteConnect


with open('C:/Users/ekans/Documents/inputs/Login_Credentials.txt','r') as a:
        content = a.readlines()
        a.close()
api_key = content[2].strip('\n')
kite = KiteConnect(api_key=api_key)

with open('C:/Users/ekans/Documents/inputs/TEXT_INSTRUMENTS.csv','w',newline='') as csvfile: 
    # creating a csv writer object 
    csvwriter = csv.writer(csvfile) 
    
    k = kite.instruments(exchange= 'NFO')
    # writing the fields 
    csvwriter.writerow(k) 
        
    # writing the data rows 
    #csvwriter.writerows(rows)
    
    
    #content = b.readlines()
    #b.close()
    #print()
    