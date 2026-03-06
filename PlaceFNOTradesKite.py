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
from FetchOptionContractName import FetchContractName
from Server_Order_Place import order
from Set_Gtt_Exit import Set_Gtt
from inputimeout import inputimeout,TimeoutOccurred
from datetime import datetime as dt, timedelta, date
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
PREVIOUSDATE = date.today() + timedelta(-1)
#If previous day is Sunday then last working day (Friday) will be 3 days prior
if PREVIOUSDATE.weekday() == SUNDAY:
    PREVIOUSDATE = date.today() + timedelta(-3)

def PlaceOrders(OrderDetails):
    order(OrderDetails)

def LoopHashOrderRequest(OrderDetails):
    for OrderType in OrderDetails:
        ContractName = [FetchContractName(OrderDetails[OrderType])]
        print(ContractName)

        #If multiple contracts returned (e.g. straddle tuple), unwrap the tuple
        if len(ContractName[0]) > 1 and len(ContractName[0]) < 3:
            ContractName = ContractName[0]

        for contract in ContractName:
            OrderDetails[OrderType]['Tradingsymbol'] = contract

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
            print("1A--NiftyStraddle_Mon_12Pm_100Sl \n 2--NiftyStraddle_Tue_11Am_110Sl  \n  7--NiftySellCall_Thu_1520Pm_50Sl \n 18--SensexStraddle_Mon_930Am_150Sl \n 19--SensexStraddle_Tue_1020Am_90Sl \n 20--SensexSellCall_Fri_1520Pm_25Sl ")
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
        PrevWkDy = dt.now().weekday() - 1
        CurrWkDy = dt.now().weekday()

        now = dt.now()
        #Condition for entering the specific trade and handling if the entry date is a market holiday
        NiftyStraddle_Thu_12Pm_100Sl =       str(now.strftime("%H:%M:%S")) == '12:00:00' and ((CurrWkDy == THURSDAY) or (PrevWkDy == WEDNESDAY and CheckForDateHoliday(PREVIOUSDATE)))
        NiftyStraddle_Fri_11Am_110Sl =       str(now.strftime("%H:%M:%S")) == '11:00:00' and ((CurrWkDy == FRIDAY)or (PrevWkDy == THURSDAY and CheckForDateHoliday(PREVIOUSDATE)))

        NiftyStraddle_Thu_12Pm_40Sl =       str(now.strftime("%H:%M:%S")) == '12:00:05' and ((CurrWkDy == THURSDAY) or (PrevWkDy == WEDNESDAY and CheckForDateHoliday(PREVIOUSDATE)))
        NiftyStraddle_Fri_11Am_70Sl =       str(now.strftime("%H:%M:%S")) == '11:00:05' and ((CurrWkDy == FRIDAY)or (PrevWkDy == THURSDAY and CheckForDateHoliday(PREVIOUSDATE)))
        

        SensexStraddle_Fri_930Am_150Sl =     str(now.strftime("%H:%M:%S")) == '09:30:00' and ((CurrWkDy == FRIDAY)   or (PrevWkDy == THURSDAY and CheckForDateHoliday(PREVIOUSDATE)))
        SensexStraddle_Mon_1020Am_90Sl =    str(now.strftime("%H:%M:%S")) == '10:20:00' and ((CurrWkDy == MONDAY) or (PrevWkDy == FRIDAY and CheckForDateHoliday(PREVIOUSDATE)))

        SensexStraddle_Fri_930Am_50Sl =     str(now.strftime("%H:%M:%S")) == '09:30:05' and ((CurrWkDy == FRIDAY)   or (PrevWkDy == THURSDAY and CheckForDateHoliday(PREVIOUSDATE)))
        SensexStraddle_Mon_1020Am_50Sl =    str(now.strftime("%H:%M:%S")) == '10:20:05' and ((CurrWkDy == MONDAY) or (PrevWkDy == FRIDAY and CheckForDateHoliday(PREVIOUSDATE)))


        NiftySellCall_Tue_1520Pm_50Sl =      str(now.strftime("%H:%M:%S")) == '15:20:00' and ((CurrWkDy == TUESDAY)or (PrevWkDy == MONDAY and CheckForDateHoliday(PREVIOUSDATE)))
        SensexSellCall_Thu_1520Pm_25Sl=      str(now.strftime("%H:%M:%S")) == '15:20:00' and ((CurrWkDy == THURSDAY)or (PrevWkDy == WEDNESDAY and CheckForDateHoliday(PREVIOUSDATE)))

        AngelNararushNiftySellCall_Wed_1000Am_100Sl =            str(now.strftime("%H:%M:%S")) == '10:00:00' and ((CurrWkDy == WEDNESDAY)or (PrevWkDy == WEDNESDAY and CheckForDateHoliday(PREVIOUSDATE)))
        AngelNararushNiftySellPut_Mon_1000Am_100Sl =             str(now.strftime("%H:%M:%S")) == '10:00:00' and ((CurrWkDy == MONDAY)or (PrevWkDy == MONDAY and CheckForDateHoliday(PREVIOUSDATE)))
        AngelNararushSensexSellCall_Wed_1000Am_25Sl = str(now.strftime("%H:%M:%S")) == '10:00:00' and ((CurrWkDy == WEDNESDAY)or (PrevWkDy == WEDNESDAY and CheckForDateHoliday(PREVIOUSDATE)))
        AngelNararushSensexSellPut_Tue_1000Am_75Sl = str(now.strftime("%H:%M:%S")) == '10:00:00' and ((CurrWkDy == TUESDAY)or (PrevWkDy == TUESDAY and CheckForDateHoliday(PREVIOUSDATE)))

        #Sell Nifty Straddle every Thursday @12pm with 100sl
        if NiftyStraddle_Thu_12Pm_100Sl or Override == '1A': 
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'NIFTY', 'Quantity': '65', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'1','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'102',
                     'StopLossOrderPlacePercent':'150','CallStrikeRequired':'True','PutStrikeRequired':'True','Hedge':'False',"OrderTag":"1NF-STR-MO-12-100"}}#,
                     
            one_shot_flag = False
            Override = False
            break

        #Sell Nifty straddle every Friday @11am with 110sl
        if NiftyStraddle_Fri_11Am_110Sl or Override == '2':
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'NIFTY', 'Quantity': '65', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'1','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'111',
                     'StopLossOrderPlacePercent':'155','CallStrikeRequired':'True','PutStrikeRequired':'True','Hedge':'False',"OrderTag":"3NF-STR-TU-11-110"}}#,

            one_shot_flag = False
            Override = False
            break

        if NiftyStraddle_Thu_12Pm_40Sl or Override == '60':
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'NIFTY', 'Quantity': '65', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'1','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'42',
                     'StopLossOrderPlacePercent':'75','CallStrikeRequired':'True','PutStrikeRequired':'True','Hedge':'False',"OrderTag":"1NF-STR-MO-12-100"}}#,

            one_shot_flag = False
            Override = False
            break

        #Sell Nifty straddle every Friday @11am with 70sl
        if NiftyStraddle_Fri_11Am_70Sl or Override == '65':
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'NIFTY', 'Quantity': '65', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'1','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'73',
                     'StopLossOrderPlacePercent':'101','CallStrikeRequired':'True','PutStrikeRequired':'True','Hedge':'False',"OrderTag":"3NF-STR-TU-11-110"}}#,

            one_shot_flag = False
            Override = False
            break         

        #Sell Nifty Call every Tuesday @1520pm with 50sl
        if NiftySellCall_Tue_1520Pm_50Sl or Override == '7':
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'NIFTY', 'Quantity': '65', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'1','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'52',
                     'StopLossOrderPlacePercent':'92','CallStrikeRequired':'True','PutStrikeRequired':'False','Hedge':'False',"OrderTag":"8NF-SC2-TH-1520-50"}}
            one_shot_flag = False
            Override = False
            break

        #Sell Sensex straddle every Friday @930 with 50sl
        if SensexStraddle_Fri_930Am_50Sl or Override == '63':
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'BFO', 'Tradingsymbol': 'SENSEX', 'Quantity': '20', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'3','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'51',
                     'StopLossOrderPlacePercent':'75','CallStrikeRequired':'True','PutStrikeRequired':'True','Hedge':'False',"OrderTag":"9SX-STR-FR-930-50"}}#,
            one_shot_flag = False
            Override = False
            break

        #Sell Sensex straddle every Monday @1020 with 50sl
        if SensexStraddle_Mon_1020Am_50Sl or Override == '64':
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'BFO', 'Tradingsymbol': 'SENSEX', 'Quantity': '20', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'3','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'51',
                     'StopLossOrderPlacePercent':'75','CallStrikeRequired':'True','PutStrikeRequired':'True','Hedge':'False',"OrderTag":"9SX-STR-MO-102-50"}}#,
            one_shot_flag = False
            Override = False
            break

        #Sell Sensex straddle every Friday @930 with 150sl
        if SensexStraddle_Fri_930Am_150Sl or Override == '18':
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'BFO', 'Tradingsymbol': 'SENSEX', 'Quantity': '20', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'3','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'151',
                     'StopLossOrderPlacePercent':'175','CallStrikeRequired':'True','PutStrikeRequired':'True','Hedge':'False',"OrderTag":"9SX-STR-FR-930-150"}}#,
            one_shot_flag = False
            Override = False
            break

        #Sell Sensex straddle every Monday @1020 with 90sl
        if SensexStraddle_Mon_1020Am_90Sl or Override == '19':
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'BFO', 'Tradingsymbol': 'SENSEX', 'Quantity': '20', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'3','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'91',
                     'StopLossOrderPlacePercent':'105','CallStrikeRequired':'True','PutStrikeRequired':'True','Hedge':'False',"OrderTag":"9SX-STR-MO-930-150"}}#,
            one_shot_flag = False
            Override = False
            break

        #Sell Sensex Call every Thursday @1520pm with 25sl
        if SensexSellCall_Thu_1520Pm_25Sl or Override == '20':
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'BFO', 'Tradingsymbol': 'SENSEX', 'Quantity': '20', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'3','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'26',
                     'StopLossOrderPlacePercent':'50','CallStrikeRequired':'True','PutStrikeRequired':'False','Hedge':'False',"OrderTag":"10SX-SC2-FR-1520-25"}}
            one_shot_flag = False
            Override = False
            break

        #Sell Nifty Put every Mon @1000 with 100sl (Angel nararush)
        if AngelNararushNiftySellPut_Mon_1000Am_100Sl or Override == '12':
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'NIFTY', 'Quantity': '65', 'Variety': 'NORMAL', 'Ordertype': 'MARKET', 'Product': 'CARRYFORWARD', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ANGEL','Netposition':'','OptionExpiryDay':'1','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'102',
                     'StopLossOrderPlacePercent':'152','CallStrikeRequired':'False','PutStrikeRequired':'True','Hedge':'False',"OrderTag":"","User":"nararush","TimePeriod":"3"}}
            one_shot_flag = False
            Override = False
            break

        #Sell Nifty Call every Wed @1000 with 100sl (Angel nararush)
        if AngelNararushNiftySellCall_Wed_1000Am_100Sl or Override == '14':
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'NIFTY', 'Quantity': '65', 'Variety': 'NORMAL', 'Ordertype': 'MARKET', 'Product': 'CARRYFORWARD', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ANGEL','Netposition':'','OptionExpiryDay':'1','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'102',
                     'StopLossOrderPlacePercent':'152','CallStrikeRequired':'True','PutStrikeRequired':'False','Hedge':'False',"OrderTag":"","User":"nararush","TimePeriod":"1"}}
            one_shot_flag = False
            Override = False
            break

        #Sell Sensex Call every Wed @1000 with 25sl (Angel nararush)
        if AngelNararushSensexSellCall_Wed_1000Am_25Sl or Override == '66':
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'BFO', 'Tradingsymbol': 'SENSEX', 'Quantity': '20', 'Variety': 'NORMAL', 'Ordertype': 'MARKET', 'Product': 'CARRYFORWARD', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ANGEL','Netposition':'','OptionExpiryDay':'3','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'27',
                     'StopLossOrderPlacePercent':'38','CallStrikeRequired':'True','PutStrikeRequired':'False','Hedge':'False',"OrderTag":"","User":"nararush","TimePeriod":"1"}}
            one_shot_flag = False
            Override = False
            break

        #Sell Sensex Put every Tue @1000 with 75sl (Angel nararush)
        if AngelNararushSensexSellPut_Tue_1000Am_75Sl or Override == '62':
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'BFO', 'Tradingsymbol': 'SENSEX', 'Quantity': '20', 'Variety': 'NORMAL', 'Ordertype': 'MARKET', 'Product': 'CARRYFORWARD', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ANGEL','Netposition':'','OptionExpiryDay':'3','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'77',
                     'StopLossOrderPlacePercent':'102','CallStrikeRequired':'False','PutStrikeRequired':'True','Hedge':'False',"OrderTag":"","User":"nararush","TimePeriod":"3"}}
            one_shot_flag = False
            Override = False
            break

        # --- Testing overrides ---
        #Place Finnifty order during active market hour for testing (Kite)
        if Override == '99':
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'FINNIFTY', 'Quantity': '65', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ZERODHA_OPTION','Netposition':'','OptionExpiryDay':'1','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'65',
                     'StopLossOrderPlacePercent':'95','CallStrikeRequired':'True','PutStrikeRequired':'False','Hedge':'False',"OrderTag":"12FN-SC-MACD-WE-65"}}
            one_shot_flag = False
            Override = False
            break

        #Place Finnifty order post market hour for testing (Kite AMO)
        if Override == '98':
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'FINNIFTY', 'Quantity': '65', 'Variety': 'AMO', 'Ordertype': 'LIMIT', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 350,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ZERODHA_OPTION','Netposition':'','OptionExpiryDay':'1','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'65',
                     'StopLossOrderPlacePercent':'95','CallStrikeRequired':'True','PutStrikeRequired':'False','Hedge':'False',"OrderTag":"12FN-SC-MACD-WE-65"}}
            one_shot_flag = False
            Override = False
            break

        #Place BankNifty AMO for narayana angel account
        if Override == '97':
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'BANKNIFTY', 'Quantity': '30', 'Variety': 'AMO', 'Ordertype': 'LIMIT', 'Product': 'CARRYFORWARD', 'Validity': 'DAY', 'Price': 200,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ANGEL','Netposition':'','OptionExpiryDay':'2','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'102',
                     'StopLossOrderPlacePercent':'152','CallStrikeRequired':'False','PutStrikeRequired':'True','Hedge':'False',"OrderTag":"","User":"nararush","TimePeriod":"3"}}
            one_shot_flag = False
            Override = False
            break

        #Buy BankNifty Call AMO for ekansh angel account
        if Override == '96':
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'BANKNIFTY', 'Quantity': '30', 'Variety': 'AMO', 'Ordertype': 'LIMIT', 'Product': 'CARRYFORWARD', 'Validity': 'DAY', 'Price': 297,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ANGEL','Netposition':'','OptionExpiryDay':'2','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'999',
                     'StopLossOrderPlacePercent':'152','CallStrikeRequired':'True','PutStrikeRequired':'False','Hedge':'MonthlyCall',"OrderTag":"7BN-LC1-FM-930-NOSL","TimePeriod":"6","User":"ekansh"}}
            one_shot_flag = False
            Override = False
            break

        #Buy NCDEX order AMO for ekansh angel account
        if Override == 'NCDEX':
            OrderDetails = {'NCDEX':{"Tradetype": "BUY", "Exchange": "NCDEX", "Tradingsymbol": "CASTOR20DEC2023", "Quantity": "1*5", "Variety": "AMO", "Ordertype": "LIMIT", "Product": "CARRYFORWARD",
                             "Validity": "DAY", "Price": 5930, "Symboltoken":"CASTOR20DEC2023", "Squareoff":"", "Stoploss":"", "Broker":"ANGEL"}}
            one_shot_flag = False
            Override = False
            break

        #Place Nifty AMO for testing (Kite)
        if Override == '95':
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'NIFTY', 'Quantity': '75', 'Variety': 'AMO', 'Ordertype': 'LIMIT', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 300,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ZERODHA_OPTION','Netposition':'','OptionExpiryDay':'1','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'65',
                     'StopLossOrderPlacePercent':'95','CallStrikeRequired':'True','PutStrikeRequired':'False','Hedge':'False',"OrderTag":"21BN-STR-TH-CFG"}}
            one_shot_flag = False
            Override = False
            break
    LoopHashOrderRequest(OrderDetails)
