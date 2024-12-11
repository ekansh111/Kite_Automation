"""
This script automates the placement of options trading orders for multiple brokers (Zerodha and Angel) 
based on predefined conditions and schedules. It reads credentials and configuration data from 
external files, checks the current day and time, and triggers specific trading strategies at 
designated hours. The script also supports user overrides, allowing for on-demand execution of 
particular trades rather than relying solely on automated timing.

Key functionalities:
- Fetches login credentials (API keys, access tokens) for trading APIs (KiteConnect and Angel SmartAPI).
- Reads from input files to determine which instruments, strikes, and option types to trade.
- Uses scheduling logic to place trades at certain times during trading days (e.g., every Monday at noon, 
  every Tuesday at 11 AM, etc.).
- Verifies market holidays and adjusts the trading schedule accordingly.
- Supports multiple strategies such as Straddles, Calls, and Puts with specified stop-loss and 
  target parameters.
- Utilizes Good Till Triggered (GTT) orders to manage exits from positions.
- Integrates logic to handle different brokers:
  - Zerodha (via Kite Connect) for placing orders and setting GTT.
  - Angel One (via SmartAPI) for placing limit and market orders, as well as GTT orders.
- Offers a user-driven override mode, where a prompt allows the user to:
  - Abort the execution.
  - Proceed with scheduled trades.
  - Modify parameters and choose a different trading strategy for immediate execution.
- Logs execution steps and trade placements to assist with debugging and verification.

Typical usage scenario:
1. The script runs continuously (or at intervals).
2. At specified times on certain weekdays, it checks if conditions are met (e.g., Monday at 12:00 PM).
3. If conditions match, it fetches the appropriate option contracts and places the orders using the 
   broker's API.
4. If GTT conditions are configured, it sets up automatic exit orders.
5. The script also allows a user to interrupt at the start and choose a different strategy by entering 
   an override code.

This setup allows for algorithmic and rules-based trading, enabling traders to predefine their 
options strategies and have them executed automatically by the script.

Note: This script relies on external modules and files (e.g., FetchOptionContractName, Set_Gtt_Exit, 
Holidays, Login_Auto3_Angel) and on having appropriate API credentials and permissions from 
brokers. It also requires careful handling of API keys and security credentials.
"""
from FetchOptionContractName import FetchOptionName
from kiteconnect import KiteConnect
from Server_Order_Place import order
from Set_Gtt_Exit import Set_Gtt
from datetime import datetime,timedelta
from inputimeout import inputimeout,TimeoutOccurred
from dateutil.relativedelta import TH,WE, relativedelta
from datetime import date
from os import abort
from Holidays import CheckForDateHoliday
from Login_Auto3_Angel import *
from AngelInstrumentTokenHandle import *
from Directories import *

MONDAY = 0
TUESDAY = 1
WEDNESDAY = 2
THURSDAY = 3
FRIDAY = 4
SUNDAY = 6
#PreviousDate in yyyy-mm-dd format
PREVIOUSDATE = date.today() + timedelta(-1)
#print(PREVIOUSDATE)
#If previous day is Sunday then last working day (friday) will be 3 days prior
if PREVIOUSDATE.weekday() == SUNDAY:
    PREVIOUSDATE = date.today() + timedelta(-3)

def PlaceOrders(OrderDetails):
    with open(KiteEkanshLoginAPIKey,'r') as a:
        api_key = a.read()
        a.close()
    kite = KiteConnect(api_key=api_key)


    with open(KiteEkanshLoginAccessToken,'r') as f:
        access_tok = f.read()
        f.close()
        #print(access_tok)
    kite.set_access_token(access_tok)

    order(OrderDetails)#OrderDetails['Tradetype'],OrderDetails['Exchange'],OrderDetails['Tradingsymbol'] ,OrderDetails['Quantity'],OrderDetails['Variety'],OrderDetails['Ordertype'],OrderDetails['Product'],OrderDetails['Validity'],OrderDetails['Price'])

#Function to iterate through the hash and place orders
def LoopHashOrderRequest(OrderDetails):
    #Iterate through the order details
    for OrderType in OrderDetails:
        #Fetch the contract name to place orders in , store as tuple for ease of looping /Multilple indexes as the dict has a child dict
        #ContractName = [FetchOptionName(OrderDetails[OrderType]['Tradingsymbol'],int(OrderDetails[OrderType]['OptionExpiryDay']),int(OrderDetails[OrderType]['OptionContractStrikeFromATMPercent']),Hedge=OrderDetails[OrderType]['Hedge'],CE_Return=OrderDetails[OrderType]['CallStrikeRequired'],PE_Return=OrderDetails[OrderType]['PutStrikeRequired'])]
        
        ContractName = [FetchOptionName(OrderDetails[OrderType])]
        #Multiple contracts can be returned by the function , but if only one contract name is returned than ensure that the variable is a tuple, to avoid the next for loop from only fetching a single char in the contract name

        #If there is only one value in the tuple then the name of the contract will in ideal circumstances have a minimum of one character and will have greater
        #than 4 characters, if there are multiple names fetched in the tuple, then for case of 2 values it will go inside loop and be extracted from the tuple
        #Can provision the max value of 3 to even more depending on the contract name
        if len(ContractName[0]) > 1 and len(ContractName[0]) < 3 :
            #print('Inside Multiple contract check'+str(ContractName[0]))
            ContractName = ContractName[0]

        #Place trades for all the contract names returned
        for range in ContractName:
            OrderDetails[OrderType]['Tradingsymbol'] = range

            #Route through different function if order needs to be placed for Angel 
            if OrderDetails[OrderType].get("Broker") == 'ANGEL':
                #Function to fetch the symbol token based on the tradingsymbol
                OrderDetails[OrderType]['Symboltoken'] = FetchAngelInstrumentSymbolToken(OrderDetails[OrderType])
                
                #Fetch the login details object
                smartApi = Login_Angel_Api(OrderDetails[OrderType])
                
                #Place a limit order for the contract
                Limit_Order_Type(smartApi,OrderDetails[OrderType])
                #time.sleep(3)
                Set_Gtt(OrderDetails[OrderType])
            else:
                #Place order for Zerodha kite terminal
                PlaceOrders(OrderDetails[OrderType])
            
                #Set GTT for the orders, do not place GTT if the trade is a hedging trade
                Set_Gtt(OrderDetails[OrderType])
    return True


if __name__ == '__main__':
    one_shot_flag = True
    print(" G--Go Ahead!  N-->Abort the execution  M-->Modify any of the parameters")
    try:
        proceed = inputimeout(timeout=5)
        if proceed in {"G","g"}:    #{} is a set
            Override = False    
        if proceed in {"M","m"}:
            print("1A--NiftyStraddle_Mon_12Pm_100Sl \n 2--NiftyStraddle_Tue_11Am_110Sl  \n  7--NiftySellCall_Thu_1520Pm_50Sl \n 18--SensexStraddle_Mon_930Am_150Sl \n 19--SensexStraddle_Tue_1020Am_135Sl \n 20--SensexSellCall_Fri_1520Pm_25Sl ")
            print("12--AngelNararushNiftySellPut_Mon_1000Am_100Sl  \n 14--AngelNararushNiftySellCall_Wed_1000Am_100Sl")
            print("Testing-->|99|FINNIFTY_RG_K, ->|98|FINNIFTY_AMO_K, ->|97|BANKNIFTY_AMO_ANGEL_NARAYANA, ->|96|BANKNIFTY_AMO_ANGEL_EK, ->|NCDEX|ANGEL_EKANSH_TV_ALERT, ->|95|NIFTY_AMO_KITE_EK ")
            Override = input("Enter the Override value \n") or False
        if proceed in {"N","n"}:
            abort()

        #In case of timeout then the script will execute with the default values    
    except TimeoutOccurred:
        Override = False

    print('Waiting to hit the entry time')
    while one_shot_flag == True:
        PrevWkDy = datetime.now().weekday() - 1
        CurrWkDy = datetime.now().weekday()

        now = datetime.now()
        #Condition for entering the specific trade and handling if the entry date is a market holiday
        NiftyStraddle_Mon_12Pm_100Sl =       str(now.strftime("%H:%M:%S")) == '12:00:00' and ((CurrWkDy == MONDAY) or (PrevWkDy == MONDAY and CheckForDateHoliday(PREVIOUSDATE)))
        #BankNiftyStraddle_Mon_0930Am_125Sl = str(now.strftime("%H:%M:%S")) == '09:30:00' and ((CurrWkDy == MONDAY  or (PrevWkDy == MONDAY and CheckForDateHoliday(PREVIOUSDATE))))
        NiftyStraddle_Tue_11Am_110Sl =       str(now.strftime("%H:%M:%S")) == '11:00:00' and ((CurrWkDy == TUESDAY)or (PrevWkDy == TUESDAY and CheckForDateHoliday(PREVIOUSDATE)))
        #MidCPNiftyStraddle_Wed_13Pm_90Sl =   str(now.strftime("%H:%M:%S")) == '13:00:00' and ((CurrWkDy == WEDNESDAY)or (PrevWkDy == WEDNESDAY and CheckForDateHoliday(PREVIOUSDATE)))
        #FINNiftyStraddle_Thu_1430Pm_50Sl =   str(now.strftime("%H:%M:%S")) == '14:30:00' and ((CurrWkDy == THURSDAY) or (PrevWkDy == THURSDAY and CheckForDateHoliday(PREVIOUSDATE)))
        #BankNiftyStraddle_Fri_930Am_100Sl =  str(now.strftime("%H:%M:%S")) == '09:30:00' and ((CurrWkDy == FRIDAY)   or (PrevWkDy == FRIDAY and CheckForDateHoliday(PREVIOUSDATE)))
        
        SensexStraddle_Mon_930Am_150Sl =     str(now.strftime("%H:%M:%S")) == '09:30:00' and ((CurrWkDy == MONDAY)   or (PrevWkDy == MONDAY and CheckForDateHoliday(PREVIOUSDATE)))
        SensexStraddle_Tue_1020Am_135Sl =    str(now.strftime("%H:%M:%S")) == '10:20:00' and ((CurrWkDy == TUESDAY)or (PrevWkDy == TUESDAY and CheckForDateHoliday(PREVIOUSDATE)))


        #BankNiftySellCall_Wed_1520Pm_50Sl =  str(now.strftime("%H:%M:%S")) == '15:20:00' and ((CurrWkDy == WEDNESDAY)or (PrevWkDy == WEDNESDAY and CheckForDateHoliday(PREVIOUSDATE)))#((CurrWkDy == WEDNESDAY) or CheckForDateHoliday(PREVIOUSDATE))
        NiftySellCall_Thu_1520Pm_50Sl =      str(now.strftime("%H:%M:%S")) == '15:20:00' and ((CurrWkDy == THURSDAY)or (PrevWkDy == THURSDAY and CheckForDateHoliday(PREVIOUSDATE)))#((CurrWkDy == THURSDAY)  or CheckForDateHoliday(PREVIOUSDATE))
        SensexSellCall_Fri_1520Pm_25Sl=      str(now.strftime("%H:%M:%S")) == '15:20:00' and ((CurrWkDy == FRIDAY)or (PrevWkDy == FRIDAY and CheckForDateHoliday(PREVIOUSDATE)))       

        #For testing
        AngelBankNiftyLongCallMonthlyFirstDayMonExpiry = str(now.strftime("%H:%M:%S")) == '16:20:00' and ((CurrWkDy == WEDNESDAY)or (PrevWkDy == WEDNESDAY and CheckForDateHoliday(PREVIOUSDATE)))


        #AngelNararushBankNiftySellCall_Tue_1000Am_100Sl =        str(now.strftime("%H:%M:%S")) == '10:00:00' and ((CurrWkDy == TUESDAY)or (PrevWkDy == TUESDAY and CheckForDateHoliday(PREVIOUSDATE)))
        AngelNararushNiftySellCall_Wed_1000Am_100Sl =            str(now.strftime("%H:%M:%S")) == '10:00:00' and ((CurrWkDy == WEDNESDAY)or (PrevWkDy == WEDNESDAY and CheckForDateHoliday(PREVIOUSDATE)))
        AngelNararushNiftySellPut_Mon_1000Am_100Sl =             str(now.strftime("%H:%M:%S")) == '10:00:00' and ((CurrWkDy == MONDAY)or (PrevWkDy == MONDAY and CheckForDateHoliday(PREVIOUSDATE)))
        #AngelNararushBankNiftySellPut_Fri_1000Am_100Sl =         str(now.strftime("%H:%M:%S")) == '10:00:00' and ((CurrWkDy == FRIDAY)or (PrevWkDy == FRIDAY and CheckForDateHoliday(PREVIOUSDATE)))
        AngelNararushSensexSellCall_Wed_1000Am_25Sl = str(now.strftime("%H:%M:%S")) == '10:00:00' and ((CurrWkDy == WEDNESDAY)or (PrevWkDy == WEDNESDAY and CheckForDateHoliday(PREVIOUSDATE)))
        AngelNararushSensexSellPut_Tue_1000Am_75Sl = str(now.strftime("%H:%M:%S")) == '10:00:00' and ((CurrWkDy == TUESDAY)or (PrevWkDy == TUESDAY and CheckForDateHoliday(PREVIOUSDATE)))
       
        #For Nifty Fetch last Thursday expiry,yyyy-mm-dd format
        LastThursdayOfMonth = (date.today()+relativedelta(day=31, weekday=TH(-1)))

        #For BankNifty Fetch last Wednesday expiry
        LastWednesdayOfMonth = (date.today()+relativedelta(day=31, weekday=WE(-1)))

        #Enter the Long Call options trade on the next day of the last weekly/monthly option expiry date
        FirstMonthNiftyCallLongDate = LastThursdayOfMonth + relativedelta(days=1)
        FirstMonthBankNiftyCallLongDate = LastWednesdayOfMonth + relativedelta(days=1)

        #Condition for entering long call option
        #Updated function to date.today
        #The long call trade should be entered on the day after the monthly option contract has expired,added a condition if the day is a holidsay, to enter on next day.
        #BankNiftyLongCallMonthlyFirstDayMonExpiry = ((date.today() == FirstMonthNiftyCallLongDate) or (CheckForDateHoliday(FirstMonthNiftyCallLongDate) and PREVIOUSDATE == FirstMonthBankNiftyCallLongDate)) and str(now.strftime("%H:%M:%S")) == '09:30:30'
        #NiftyLongCallMonthlyFirstDayMonExpiry =     ((date.today() == FirstMonthNiftyCallLongDate )    or (CheckForDateHoliday(FirstMonthNiftyCallLongDate)     and PREVIOUSDATE == FirstMonthNiftyCallLongDate))     and str(now.strftime("%H:%M:%S")) == '09:30:55'

        #AngeNiftySellPut_Mon_1000Am_100Sl = str(now.strftime("%H:%M:%S")) == '10:00:00' and ((CurrWkDy == MONDAY  or (PrevWkDy == MONDAY and CheckForDateHoliday(PREVIOUSDATE))))

        #Sell Nifty Straddle every monday @ 12pm with 100sl
        if NiftyStraddle_Mon_12Pm_100Sl or Override == '1A': 
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'NIFTY', 'Quantity': '50', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'3','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'102',
                     'StopLossOrderPlacePercent':'150','CallStrikeRequired':'True','PutStrikeRequired':'True','Hedge':'False',"OrderTag":"1NF-STR-MO-12-100"}}#,
                     
                     #'Hedge':{'Tradetype': 'BUY', 'Exchange': 'NFO', 'Tradingsymbol': 'NIFTY', 'Quantity': '50', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     #'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'3','OptionContractStrikeFromATMPercent':'4','Trigger':'1','StopLossTriggerPercent':'102',
                     #'StopLossOrderPlacePercent':'150','CallStrikeRequired':'False','PutStrikeRequired':'True','Hedge':'True',"OrderTag":"1NF-STRH-MO-12-100"}}
            one_shot_flag == False
            Override = False
            #print(OrderDetails['Straddle']['Tradingsymbol'])
            break   

        #Sell N straddle every Tuesday @11am with 110sl
        if NiftyStraddle_Tue_11Am_110Sl or Override == '2': 
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'NIFTY', 'Quantity': '50', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'3','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'111',
                     'StopLossOrderPlacePercent':'155','CallStrikeRequired':'True','PutStrikeRequired':'True','Hedge':'False',"OrderTag":"3NF-STR-TU-11-110"}}#,
                     
            one_shot_flag == False
            Override = False
            #print(OrderDetails['Straddle']['Tradingsymbol'])
            break  
        

        #Sell N Call every Thursday @1520pm with 50sl
        if NiftySellCall_Thu_1520Pm_50Sl or Override == '7': 
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'NIFTY', 'Quantity': '50', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'3','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'52',
                     'StopLossOrderPlacePercent':'92','CallStrikeRequired':'True','PutStrikeRequired':'False','Hedge':'False',"OrderTag":"8NF-SC2-TH-1520-50"}}
            one_shot_flag == False
            Override = False
            #print(OrderDetails['Straddle']['Tradingsymbol'])
            break  
        

        #Sell Sensex straddle every Monday @930 with 100sl
        if SensexStraddle_Mon_930Am_150Sl or Override == '18': 
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'BFO', 'Tradingsymbol': 'SENSEX', 'Quantity': '10', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'4','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'151',
                     'StopLossOrderPlacePercent':'175','CallStrikeRequired':'True','PutStrikeRequired':'True','Hedge':'False',"OrderTag":"9SX-STR-MO-930-150"}}#,
            one_shot_flag == False
            Override = False
            #print(OrderDetails['Straddle']['Tradingsymbol'])
            break

        #Sell Sensex straddle every Tuesday @1020 with 135sl
        if SensexStraddle_Tue_1020Am_135Sl or Override == '19': 
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'BFO', 'Tradingsymbol': 'SENSEX', 'Quantity': '10', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'4','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'136',
                     'StopLossOrderPlacePercent':'160','CallStrikeRequired':'True','PutStrikeRequired':'True','Hedge':'False',"OrderTag":"9SX-STR-MO-930-150"}}#,
            one_shot_flag == False
            Override = False
            #print(OrderDetails['Straddle']['Tradingsymbol'])
            break

        #Sell Sensex Call every Friday @1520pm with 25sl
        if SensexSellCall_Fri_1520Pm_25Sl or Override == '20': 
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'BFO', 'Tradingsymbol': 'SENSEX', 'Quantity': '10', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'4','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'26',
                     'StopLossOrderPlacePercent':'50','CallStrikeRequired':'True','PutStrikeRequired':'False','Hedge':'False',"OrderTag":"10SX-SC2-FR-1520-25"}}
            one_shot_flag == False
            Override = False
            #print(OrderDetails['Straddle']['Tradingsymbol'])
            break  

        #Sell N Put every Mon @1000 with 100sl
        if AngelNararushNiftySellPut_Mon_1000Am_100Sl or Override == '12': 
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'NIFTY', 'Quantity': '50', 'Variety': 'NORMAL', 'Ordertype': 'MARKET', 'Product': 'CARRYFORWARD', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ANGEL','Netposition':'','OptionExpiryDay':'3','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'102',
                     'StopLossOrderPlacePercent':'152','CallStrikeRequired':'False','PutStrikeRequired':'True','Hedge':'False',"OrderTag":"","User":"nararush","TimePeriod":"3"}}
            one_shot_flag == False
            Override = False
            #print(OrderDetails['Straddle']['Tradingsymbol'])
            break

        #Sell N Call every Wed @1000 with 100sl
        if AngelNararushNiftySellCall_Wed_1000Am_100Sl or Override == '14': 
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'NIFTY', 'Quantity': '50', 'Variety': 'NORMAL', 'Ordertype': 'MARKET', 'Product': 'CARRYFORWARD', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ANGEL','Netposition':'','OptionExpiryDay':'3','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'102',
                     'StopLossOrderPlacePercent':'152','CallStrikeRequired':'True','PutStrikeRequired':'False','Hedge':'False',"OrderTag":"","User":"nararush","TimePeriod":"1"}}
            one_shot_flag == False
            Override = False
            #print(OrderDetails['Straddle']['Tradingsymbol'])
            break
        
        
        #Sell Sensex Call every Wed @1000 with 100sl
        if AngelNararushSensexSellCall_Wed_1000Am_25Sl or Override == '61': 
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'BFO', 'Tradingsymbol': 'SENSEX', 'Quantity': '10', 'Variety': 'NORMAL', 'Ordertype': 'MARKET', 'Product': 'CARRYFORWARD', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ANGEL','Netposition':'','OptionExpiryDay':'4','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'27',
                     'StopLossOrderPlacePercent':'38','CallStrikeRequired':'True','PutStrikeRequired':'False','Hedge':'False',"OrderTag":"","User":"nararush","TimePeriod":"4"}}
            one_shot_flag == False
            Override = False
            #print(OrderDetails['Straddle']['Tradingsymbol'])
            break

        #Sell Sensex Put every Fri @1000 with 100sl
        if AngelNararushSensexSellPut_Tue_1000Am_75Sl or Override == '62': 
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'BFO', 'Tradingsymbol': 'SENSEX', 'Quantity': '10', 'Variety': 'NORMAL', 'Ordertype': 'MARKET', 'Product': 'CARRYFORWARD', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ANGEL','Netposition':'','OptionExpiryDay':'4','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'77',
                     'StopLossOrderPlacePercent':'102','CallStrikeRequired':'False','PutStrikeRequired':'True','Hedge':'False',"OrderTag":"","User":"nararush","TimePeriod":"3"}}
            one_shot_flag == False
            Override = False
            #print(OrderDetails['Straddle']['Tradingsymbol'])
            break

        ######################################################################################################################################################################################################################################################
        #testing purpose
        #Place Finifty order during active market hour for testing with GTT order set for Kite
        if  Override == '99':
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'FINNIFTY', 'Quantity': '50', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ZERODHA_OPTION','Netposition':'','OptionExpiryDay':'1','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'65',
                     'StopLossOrderPlacePercent':'95','CallStrikeRequired':'True','PutStrikeRequired':'False','Hedge':'False',"OrderTag":"12FN-SC-MACD-WE-65"}}    
            one_shot_flag == False
            Override = False
            #print(OrderDetails['Straddle']['Tradingsymbol'])
            break

        #testing purpose
        #Place Finifty order post market hour for testing with GTT order set for Kite
        if  Override == '98':
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'FINNIFTY', 'Quantity': '50', 'Variety': 'AMO', 'Ordertype': 'LIMIT', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 350,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ZERODHA_OPTION','Netposition':'','OptionExpiryDay':'1','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'65',
                     'StopLossOrderPlacePercent':'95','CallStrikeRequired':'True','PutStrikeRequired':'False','Hedge':'False',"OrderTag":"12FN-SC-MACD-WE-65"}}    
            one_shot_flag == False
            Override = False
            #print(OrderDetails['Straddle']['Tradingsymbol'])
            break

        #Place BankNifty order POST market hour for testing with GTT order set for Kite for narayana angel account
        if Override == '97': 
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'BANKNIFTY', 'Quantity': '15', 'Variety': 'AMO', 'Ordertype': 'LIMIT', 'Product': 'CARRYFORWARD', 'Validity': 'DAY', 'Price': 200,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ANGEL','Netposition':'','OptionExpiryDay':'2','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'102',
                     'StopLossOrderPlacePercent':'152','CallStrikeRequired':'False','PutStrikeRequired':'True','Hedge':'False',"OrderTag":"","User":"nararush","TimePeriod":"3"}}
            one_shot_flag == False
            Override = False
            #print(OrderDetails['Straddle']['Tradingsymbol'])
            break

        #Buy BankNifty Call post market hours for ekansh angel account
        if  Override == '96': #variety,ORDERTYPE,PRICE
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'BANKNIFTY', 'Quantity': '15', 'Variety': 'AMO', 'Ordertype': 'LIMIT', 'Product': 'CARRYFORWARD', 'Validity': 'DAY', 'Price': 297,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ANGEL','Netposition':'','OptionExpiryDay':'2','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'999',
                     'StopLossOrderPlacePercent':'152','CallStrikeRequired':'True','PutStrikeRequired':'False','Hedge':'MonthlyCall',"OrderTag":"7BN-LC1-FM-930-NOSL","TimePeriod":"6","User":"ekansh"}}
            #one_shot_flag == False
            Override = False
            #print(OrderDetails['Straddle']['Tradingsymbol'])
            break

        #Buy NCDEX order post market hours for ekansh using existing format
        if  Override == 'NCDEX': 
            OrderDetails = {'NCDEX':{"Tradetype": "BUY", "Exchange": "NCDEX", "Tradingsymbol": "CASTOR20DEC2023", "Quantity": "1*5", "Variety": "AMO", "Ordertype": "LIMIT", "Product": "CARRYFORWARD",
                             "Validity": "DAY", "Price": 5930, "Symboltoken":"CASTOR20DEC2023", "Squareoff":"", "Stoploss":"", "Broker":"ANGEL"}}
            #one_shot_flag == False
            Override = False
            #print(OrderDetails['Straddle']['Tradingsymbol'])
            break  

        #testing purpose
        #Place Nifty order post market hour for testing with GTT order set for Kite
        if  Override == '95':
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'NIFTY', 'Quantity': '50', 'Variety': 'AMO', 'Ordertype': 'LIMIT', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 350,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ZERODHA_OPTION','Netposition':'','OptionExpiryDay':'2','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'65',
                     'StopLossOrderPlacePercent':'95','CallStrikeRequired':'True','PutStrikeRequired':'False','Hedge':'False',"OrderTag":"12FN-SC-MACD-WE-65"}}    
            one_shot_flag == False
            Override = False
            #print(OrderDetails['Straddle']['Tradingsymbol'])
            break
    LoopHashOrderRequest(OrderDetails)



            