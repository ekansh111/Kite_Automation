from email import header
from operator import index
import pandas as pd    
import logging
from os import abort
import string
from kiteconnect import KiteConnect
from datetime import datetime,timedelta
from datetime import date
import re
import string
from dateutil.relativedelta import TH, relativedelta
import datetime
from Directories import *
#logging.basicConfig(level=logging.DEBUG) #This line is commented to prevent the connection debug message from being printed
n = int(input("How many percent OTM option values to be fetched"))
ltp = 0
ATM_ltp = 0
year = 0;month=0;day=0;index_val = [];df_inst_token_ce = [0]*n*2;df_inst_token_pe = [0]*n*2


with open(KiteEkanshLoginAPIKey,'r') as a:
        api_key = a.read()
        a.close()
kite = KiteConnect(api_key=api_key)


with open(KiteEkanshLoginAccessToken,'r') as f:
    access_tok = f.read()
    f.close()
    #print(access_tok)
kite.set_access_token(access_tok)    

def get_ltp():
    Token = {260105:'NIFTY BANK'}
    
    for val in Token:
        price = kite.ltp('NSE:' + Token[val])
        #print(price)
        ltp = price['NSE:'+Token[val]]['last_price']
        print("BankNifty LTP:"+str(ltp))
    return ltp

ATM_ltp = int(round(get_ltp(),-2))



def fetch_date():
    Thursday_date = date.today()
    while Thursday_date.weekday() != 3:
        Thursday_date += timedelta(1)  
    y0= Thursday_date.strftime("%y")
    year = int(y0)

    #Get the month for the option contract
    Thursday_date = date.today()
    while Thursday_date.weekday() != 3:
        Thursday_date += timedelta(1)  
    m0= Thursday_date.strftime("%m")#find out using the days left to next month, if the next month value needs to be added. This can happen during the last week of the month when the next month's weekly contract needs to selected.
    month=int(m0)#month has to be converted into an integer because it cannot have 0 suffixing it if it is a single digit month eg in the contract 

    #get the date of next thursday
    Thursday_date = date.today()
    while Thursday_date.weekday() != 3:
        Thursday_date += timedelta(1)
        d0= Thursday_date.strftime("%d")
        day=d0
    if date.today().weekday() ==3: 
        day= date.today().strftime("%d")
    
    def check_last_thursday(d0,m0,y0):
        k = (datetime.date.today()+relativedelta(day=31, weekday=TH(-1)))
        dl = k.strftime("%d")
        ml = k.strftime("%m")
        yl = k.strftime("%y")

        if((dl == d0) and (ml == m0) and (yl == y0) ):
            return True
        #print(dl+ml+yl)
        return False        
    
    if(check_last_thursday(d0,m0,y0)):
        d0 = ''
        m0 = Thursday_date.strftime("%b").upper()#needs to be in string format as it has to be passed in %d%d format , i.e 3rd March will go as '03'/3
    
    return y0+m0+d0

#print(fetch_date())

   # Read the file    

data = pd.read_csv("D:/instrument_input/instruments.csv", low_memory=False)    
    
# Output the number of rows 
df = pd.DataFrame(data,columns = ['instrument_token','tradingsymbol'])
#df_filter = df[df.tradingsymbol.contains('BANK*')]
#print(df)

def fetch_index_val(Price):
    if Price > ATM_ltp:
        regex = "BANKNIFTY"+fetch_date()+str(Price)+"CE"  

    else:
        regex = "BANKNIFTY"+fetch_date()+str(Price)+"PE"   #"BANKNIFTY22FEB30400CE"#
    
    series = df['tradingsymbol']
    df1 = series.str.contains(regex)
    df2 = df1.loc[df1 == True]
    return df2.index

for i in range(1,int(n)):
    ATM_HEDGE_CE = round(int(ATM_ltp*((100+i)/100)),-2)
    ATM_HEDGE_PE = round(int(ATM_ltp*((100-i)/100)),-2)
    index_val_ce = (fetch_index_val(ATM_HEDGE_CE))
    index_val_pe = (fetch_index_val(ATM_HEDGE_PE))#index_val_pe = fetch_index_val(ATM_HEDGE_PE)
    #print(ATM_HEDGE_CE,ATM_HEDGE_PE)
    df_inst_token_ce[i] = df.iloc[index_val_ce].to_string(index = False,header = False)
    df_inst_token_pe[i] = df.iloc[index_val_pe].to_string(index = False,header = False)

    #print(df_inst_token_ce)
    #print(df_inst_token_pe)
#index_val = fetch_index_val()
#print(df2.index)

#print(df_inst_token_ce[2:3])
l = str(df_inst_token_ce[2:3]).split()[1].replace("']","")
#print(str(l))

def fetch_ltp_options(df_inst_token_ce,df_inst_token_pe):
    val =1
    
    for val in range(1,n):
        call_option_name = str(df_inst_token_ce[val:val+1]).split()[1].replace("']","")
        price_ce = kite.ltp('NFO:' + call_option_name)
        ltp_ce = price_ce['NFO:'+ call_option_name]['last_price']
        
        print("Call Option " +" "+ str(val) +"%" +" "+" Hedge"+ call_option_name+ "  "+"LTP:"+ " " + str(ltp_ce) + "  "+"Hedging Buy Cost:" + "  "+ str(int(ltp_ce)*25))
    for val in range(1,n):
        put_option_name = str(df_inst_token_pe[val:val+1]).split()[1].replace("']","")
        price_pe = kite.ltp('NFO:' + put_option_name)
        ltp_pe = price_pe['NFO:'+put_option_name]['last_price']
        
        print("Put Option " +" "+ str(val) +"%" +" "+" Hedge"+  put_option_name+ "  "+"LTP:"+ " " + str(ltp_pe) + "  "+"Hedging Buy Cost:" + "  "+ str(int(ltp_pe)*25))
    
    return ltp_ce,ltp_pe
fetch_ltp_options(df_inst_token_ce,df_inst_token_pe)
kill = input("Click on a key to proceed")

#print("Total rows: {0}".format(len(data)))    


#print(get_ltp())    
# See which headers are available    
#print(list(data))    

#if __name__ == '__main__':

