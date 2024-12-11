#from calendar import Wednesday
import logging
from os import abort
import string
from kiteconnect import KiteConnect
from datetime import datetime,timedelta
from datetime import date
from ContractDetails import ContractStrikeValue
from Set_Gtt_Exit import Set_Gtt
from inputimeout import inputimeout,TimeoutOccurred
from dateutil.relativedelta import TH,MO,TU,WE,FR, relativedelta
import time
from Holidays import CheckForDateHoliday
from Directories import *

#Function to fetch the option name of the script
def FetchOptionName(OrderDetails):

    #print(OrderDetails)
    #Fetch the required varables from the hash
    IndexName                    = OrderDetails['Tradingsymbol']
    ExpiryDayInt                 = int(OrderDetails['OptionExpiryDay'])
    ContractStrikeFromATMPercent = int(OrderDetails['OptionContractStrikeFromATMPercent'])
    Hedge                        = OrderDetails['Hedge']
    CE_Return                    = OrderDetails['CallStrikeRequired']
    PE_Return                    = OrderDetails['PutStrikeRequired']
    Broker                       = OrderDetails.get("Broker")
    #order_details_fetch.get("Broker") == 'ANGEL':

    #Code block to check if the expiry day is a holiday or not###########################################################################################################################################################################################
    ExpiryDate = date.today() 
    while ExpiryDate.weekday() != ExpiryDayInt:
        ExpiryDate += timedelta(1)

    IdealExpiryDateForContract = ExpiryDate
    #print(IdealExpiryDateForContract)
    CheckIfExpiryDateIsHoliday = CheckForDateHoliday(IdealExpiryDateForContract)
    #print(CheckIfExpiryDateIsHoliday)
    if str(CheckIfExpiryDateIsHoliday) == 'True':
        #If the Expiry date is on Monday then the contract will expire on friday
        if ExpiryDayInt == 0:
            ExpiryDayInt = 4
            if Broker == 'ANGEL':
                GTTOrderTimePeriodExpiryDay = int(OrderDetails.get("TimePeriod"))
                GTTOrderTimePeriodExpiryDay = GTTOrderTimePeriodExpiryDay - 3
                OrderDetails['TimePeriod']  = GTTOrderTimePeriodExpiryDay   
        else:
            #If the expiry day is a holiday then fetch the prior date
            ExpiryDayInt = ExpiryDayInt - 1
            
            if Broker == 'ANGEL':
                GTTOrderTimePeriodExpiryDay = int(OrderDetails.get("TimePeriod"))
                GTTOrderTimePeriodExpiryDay = GTTOrderTimePeriodExpiryDay - 1
                OrderDetails['TimePeriod']  = GTTOrderTimePeriodExpiryDay 
    ######################################################################################################################################################################################################################################################

    #Contract Strike should not be overriden by default, Unless specified in the request
    ContractStrikeOverride = 'False'

    ContractStrikeOverridePrice  = False or OrderDetails.get("ContractStrikeOverridePrice")
    
    #If the ContractStrikeOverridePrice is not entry and is not null, then in that case it must be having the ltp ovveride value for 
    #the option entry contract
    if str(OrderDetails.get("ContractStrikeOverridePrice")) != 'Entry' and str(OrderDetails.get("ContractStrikeOverridePrice")) != str(None):
        ContractStrikeOverride       = 'True'
        #ltp = ContractStrikeOverridePrice


    #print(IndexName,ExpiryDayInt,ContractStrikeFromATMPercent,Hedge,CE_Return,PE_Return)
    #Configure the API for fetching the ltp value
    with open(KiteEkanshLogin,'r') as a:
        content = a.readlines()
        a.close()
    api_key = content[2].strip('\n')
    kite = KiteConnect(api_key=api_key)



    with open(KiteEkanshLoginAccessToken,'r') as f:
        access_tok = f.read()
        f.close()
        #print(access_tok)
    kite.set_access_token(access_tok)

    #Get the date of this year ,month and day
    ExpiryDate = date.today()
    
    #If the contract expiry weekday is the same as the weekday on which the trade is placed, then fetch the next weekend date,
    #to place order in the corresponding next weekday contract.
    if  ExpiryDate.weekday() == ExpiryDayInt:
        ExpiryDate += timedelta(7)

    # weekday() can be used to retrieve the day of the week. The datetime.today() method returns the current date, and the weekday() method returns the day of the week as an integer where Monday is indexed as 0 and Sunday is 6.
    #Fetch the expiry date for the contract
    while ExpiryDate.weekday() != ExpiryDayInt:
        #Since the options expire on Expiry their namefield will have the corresponding date field of that day
        ExpiryDate += timedelta(1)  

    #For the provided expiry day(in int) fetch the day of the week in char format and then fetch the last date for the final instance of the particular day in the month
    HashOfDays = {0:MO,1:TU,2:WE,3:TH,4:FR}
    ExpiryDay = HashOfDays[ExpiryDayInt]

    #Fetch the last Expiry date of the month,Needs correction for various different contracts
    last_day = (date.today()+relativedelta(day=31, weekday=ExpiryDay(-1)))
    
    #if the last Expiry day comes in next year then the year will be rolled over
    y0= ExpiryDate.strftime("%y")
    
    #No longeer in use as the format has been changed #find out using the days left to next month, if the next month value needs to be added. This can happen during the last week of the month when the next month's weekly contract needs to selected.
    m0 = ExpiryDate.strftime("%m")
    
    #month has to be converted into an integer because it cannot have 0 suffixing it if it is a single digit month eg in the contract    
    month = int(m0)#[0]#Get the first letter of the month for new format

    if month > 9:
        m0 = (ExpiryDate.strftime("%b")).upper()
        print('First day of the month' + str(m0))
        month = m0[0]

    #m0 = (ExpiryDate.strftime("%b")).upper()#Fetch the name of the month of the option series to be executed
    d0= ExpiryDate.strftime("%d")
    year = int(y0)
    day=d0

    #Check if the day is last Expiry day of the month (Since Expiry day is the expiry day for options)
    #Even if the last day turns out to be an holiday, no issues since the prior day will also be last instance for that month and Monthly contract name willl be used
    #Post dec 2024 , even Angel will need same changes
    if (ExpiryDate == last_day): #and (Broker != 'ANGEL'):
    #If true then need to change the value for month and day as the name format of the option contract(Month expiry Option ) changes
        month=(date.today().strftime("%b")).upper()
        day=''
    ########################################################################################################################################################################################################################

    #If the entry needs to be taken in the next month Call option, Then fetch the next month name for contract 
    if(Hedge == 'MonthlyCall') and (Broker != 'ANGEL'):
        #Fetch the next month name
        month=((date.today() + relativedelta(months=1)).strftime("%b") ).upper()
        day=''
    
    #print(kite.instruments(exchange='NSE'))
    #Fetch the LTP based on the Indexes for preparing the contracts
    if IndexName == 'BANKNIFTY' and (ContractStrikeOverride != 'True'):
        ##################################################################################################################################
        #Find the current value of the bank nifty index
        Banknifty_index = {260105:'NIFTY BANK'}
        
        for val in Banknifty_index:
            time.sleep(0.1)
            #this will send ohlc price in dictionary format
            price = kite.ltp('NSE:' + Banknifty_index[val])
            #print(price)
            #to get ltp of whichever stick is declared in token
            ltp = price['NSE:'+Banknifty_index[val]]['last_price']
            print("BankNifty LTP:"+str(ltp))            

        ##################################################################################################################################

    elif IndexName == 'NIFTY' and (ContractStrikeOverride != 'True'):
        ##################################################################################################################################
        #Find the current value of the bank nifty index
        NIFTY_index = {256265:'NIFTY 50'}
        
        for val in NIFTY_index:
            time.sleep(0.1)
            price = kite.ltp('NSE:' + NIFTY_index[val])#this will send ohlc price in dictionary format
            #print(price)
            ltp = price['NSE:'+NIFTY_index[val]]['last_price']#to get ltp of whichever stick is declared in token
            print("NIFTY LTP:"+str(ltp))
                

        ##################################################################################################################################

    elif IndexName == 'FINNIFTY' and (ContractStrikeOverride != 'True'):
        ##################################################################################################################################
        #Find the current value of the bank nifty index
        NIFTY_index = {257801:'NIFTY FIN SERVICE'}
        
        for val in NIFTY_index:
            time.sleep(0.1)
            price = kite.ltp('NSE:' + NIFTY_index[val])#this will send ohlc price in dictionary format
            #print(price)
            ltp = price['NSE:'+NIFTY_index[val]]['last_price']#to get ltp of whichever stick is declared in token
            print("FINNIFTY LTP:"+str(ltp))
                

        ##################################################################################################################################

    elif IndexName == 'MIDCPNIFTY' and (ContractStrikeOverride != 'True'):
        ##################################################################################################################################
        #Find the current value of the bank nifty index
        NIFTY_index = {288009:'NIFTY MID SELECT'}
        
        for val in NIFTY_index:
            time.sleep(0.1)
            price = kite.ltp('NSE:' + NIFTY_index[val])#this will send ohlc price in dictionary format
            #print(price)
            ltp = price['NSE:'+NIFTY_index[val]]['last_price']#to get ltp of whichever stick is declared in token
            print("MIDCPNIFTY LTP:"+str(ltp))
                

        ##################################################################################################################################     

    elif IndexName == 'SENSEX' and (ContractStrikeOverride != 'True'):
        ##################################################################################################################################
        #Find the current value of the bank nifty index
        NIFTY_index = {265:'SENSEX'}
        
        for val in NIFTY_index:
            time.sleep(0.1)
            price = kite.ltp('BSE:' + NIFTY_index[val])#this will send ohlc price in dictionary format
            #print(price)
            ltp = price['BSE:'+NIFTY_index[val]]['last_price']#to get ltp of whichever stick is declared in token
            print("SENSEX LTP:"+str(ltp))
                

        ##################################################################################################################################      

    #Override the ltp value If the contract name needs to be Named according to ltp(when the contract position was entered) provided in the request
    if ContractStrikeOverride == 'True':
        ltp = float(ContractStrikeOverridePrice)
    #print('ltp--->' + str(ltp))
    #This will round to the nearest hundrend place so that we can select the nearst ATM contract
    ATM_ltp = int(round(ltp,-2))
    #print(ATM_ltp)
    #-2 to round it to the nearest hundreds place(since that is the steps in which options are priced)
    #ATM_CE_Strike = round(int(ATM_ltp*((100+int(ContractStrikeFromATMPercent))/100)),-2)
    #ATM_PE_Strike = round(int(ATM_ltp*((100-int(ContractStrikeFromATMPercent))/100)),-2)
    ATM_CE_Strike,ATM_PE_Strike = ContractStrikeValue(ContractStrikeFromATMPercent,ATM_ltp,IndexName)
    #Date Format for Kite API 
    DateFormat = str(year)+ str(month) +str(day)

    if Broker == 'ANGEL':
        if IndexName != 'SENSEX':
            month = (ExpiryDate.strftime("%b")).upper()
            DateFormat = str(day)+str(month)+str(year)
        else:
            DateFormat = str(year) +str(month) + str(day)

    if IndexName == 'BANKNIFTY':
        #Append the dates and the script name to create the complete contract names to be Traded
        ATM_CALL = 'BANKNIFTY'+ DateFormat +str(ATM_CE_Strike)+'CE'
        ATM_PUT = 'BANKNIFTY'+ DateFormat +str(ATM_PE_Strike)+'PE'

    elif IndexName == 'NIFTY':
        #Append the dates and the script name to create the complete contract names to be Traded
        ATM_CALL = 'NIFTY'+ DateFormat +str(ATM_CE_Strike)+'CE'
        ATM_PUT = 'NIFTY'+ DateFormat +str(ATM_PE_Strike)+'PE'
    
    elif IndexName == 'FINNIFTY':
        #Append the dates and the script name to create the complete contract names to be Traded
        ATM_CALL = 'FINNIFTY'+ DateFormat +str(ATM_CE_Strike)+'CE'
        ATM_PUT = 'FINNIFTY'+ DateFormat +str(ATM_PE_Strike)+'PE' 

    elif IndexName == 'MIDCPNIFTY':
        #Append the dates and the script name to create the complete contract names to be Traded
        ATM_CALL = 'MIDCPNIFTY'+ DateFormat +str(ATM_CE_Strike)+'CE'
        ATM_PUT = 'MIDCPNIFTY'+ DateFormat +str(ATM_PE_Strike)+'PE'

    elif IndexName == 'SENSEX':
        #Append the dates and the script name to create the complete contract names to be Traded
        ATM_CALL = 'SENSEX'+ DateFormat +str(ATM_CE_Strike)+'CE'
        ATM_PUT = 'SENSEX'+ DateFormat +str(ATM_PE_Strike)+'PE'
    print(year,month,day,ATM_ltp,ATM_CALL,ATM_PUT,CE_Return,PE_Return)

    #Return Only the required contracts
    if CE_Return == 'True' and PE_Return=='False':
        print(ATM_CALL)
        return ATM_CALL
    elif PE_Return == 'True' and CE_Return=='False':
        print(ATM_PUT)
        return ATM_PUT
    elif PE_Return == 'True' and CE_Return=='True':
        print(ATM_CALL,ATM_PUT)
        return ATM_CALL,ATM_PUT



if __name__ == '__main__':

    #FetchOptionName('BANKNIFTY',2,0,False,False)
    #FetchOptionName('NIFTY',3,0,False,False)
    #FetchOptionName('FINNIFTY',1,0,False,False)
    '''   IndexName                    = OrderDetails['Tradingsymbol']
    ExpiryDayInt                 = int(OrderDetails['OptionExpiryDay'])
    ContractStrikeFromATMPercent = int(OrderDetails['OptionContractStrikeFromATMPercent'])
    Hedge                        = OrderDetails['Hedge']
    CE_Return                    = OrderDetails['CallStrikeRequired']
    PE_Return                    = OrderDetails['PutStrikeRequired']'''
    OrderDetails = {'Tradingsymbol':'MIDCPNIFTY','OptionExpiryDay':0,'OptionContractStrikeFromATMPercent':0,'Hedge':'False',
                    'CallStrikeRequired':'True','PutStrikeRequired':'False'} 
    '''OrderDetails['Tradingsymbol'] = 'MIDCPNIFTY'
    OrderDetails['OptionExpiryDay'] = 0
    OrderDetails['OptionContractStrikeFromATMPercent'] = 0
    OrderDetails['Hedge'] = 'False'
    OrderDetails['CallStrikeRequired'] = 'True'
    OrderDetails['PutStrikeRequired']  = 'False' '''

    k = FetchOptionName(OrderDetails)
    #print(k[0])
