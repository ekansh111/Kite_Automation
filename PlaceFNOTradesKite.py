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
    #print('Function called multiple times?')
    #Iterate through the order details
    for OrderType in OrderDetails:
        #Fetch the contract name to place orders in , store as tuple for ease of looping /Multilple indexes as the dict has a child dict
        #ContractName = [FetchOptionName(OrderDetails[OrderType]['Tradingsymbol'],int(OrderDetails[OrderType]['OptionExpiryDay']),int(OrderDetails[OrderType]['OptionContractStrikeFromATMPercent']),Hedge=OrderDetails[OrderType]['Hedge'],CE_Return=OrderDetails[OrderType]['CallStrikeRequired'],PE_Return=OrderDetails[OrderType]['PutStrikeRequired'])]
        
        ContractName = [FetchOptionName(OrderDetails[OrderType])]
        #Multiple contracts can be returned by the function , but if only one contract name is returned than ensure that the variable is a tuple, to avoid the next for loop from only fetching a single char in the contract name
        #print(ContractName)
        #If there is only one value in the tuple then the name of the contract will in ideal circumstances have a minimum of one character and will have greater
        #than 4 characters, if there are multiple names fetched in the tuple, then for case of 2 values it will go inside loop and be extracted from the tuple
        #Can provision the max value of 3 to even more depending on the contract name
        if len(ContractName[0]) > 1 and len(ContractName[0]) < 3 :
            #print('Inside Multiple contract check'+str(ContractName[0]))
            ContractName = ContractName[0]

        #Place trades for all the contract names returned
        for range in ContractName:
            OrderDetails[OrderType]['Tradingsymbol'] = range
            #print(range)
            #_#modify condition for angel

            #Route through different function if order needs to be placed for Angel 
            if OrderDetails[OrderType].get("Broker") == 'ANGEL':
                #Function to fetch the symbol token based on the tradingsymbol
                OrderDetails[OrderType]['Symboltoken'] = FetchAngelInstrumentSymbolToken(OrderDetails[OrderType])
                
                #Fetch the login details object
                smartApi = Login_Angel_Api(OrderDetails[OrderType])
                
                #Place a limit order for the contract
                Limit_Order_Type(smartApi,OrderDetails[OrderType])
                
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
            print("1A--NiftyStraddle_Mon_12Pm_100Sl \n 1B--BankNiftyStraddle_Mon_1030Am_125Sl \n 2--NiftyStraddle_Tue_11Am_110Sl \n 3--MidCPNiftyStraddle_Wed_13Pm_90Sl \n 4--FINNiftyStraddle_Thu_1430Pm_50Sl \n 5--BankNiftyStraddle_Fri_930Am_100Sl \n  6--BankNiftySellCall_Wed_1520Pm_50Sl \n  7--NiftySellCall_Thu_1520Pm_50Sl \n 8--NiftyLongCallMonthlyFirstDayMonExpiry \n 9--BankNiftyLongCallMonthlyFirstDayMonExpiry")
            print("12--AngelNararushNiftySellPut_Mon_1000Am_100Sl \n  17--AngelNararushBankNiftySellCall_Tue_1000Am_100Sl  \n 14--AngelNararushNiftySellCall_Wed_1000Am_100Sl \n 15--AngelNararushBankNiftySellPut_Fri_1000Am_100Sl")
            print("Testing-->|99|FINNIFTY_RG_K, ->|98|FINNIFTY_AMO_K, ->|97|BANKNIFTY_AMO_ANGEL_NARAYANA, ->|96|BANKNIFTY_AMO_ANGEL_EK, ->|NCDEX|ANGEL_EKANSH_TV_ALERT, ->|95|NIFTY_AMO_KITE_EK ")
            Override = input("Enter the Override value \n") or False
        if proceed in {"N","n"}:
            abort()

        #In case of timeout then the script will execute with the default values    
    except TimeoutOccurred:
        Override = False

    #Hash consisting of order details,Modify Variety and OrderType and price
    '''OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'MIDCPNIFTY', 'Quantity': '75', 'Variety': 'AMO', 'Ordertype': 'LIMIT', 'Product': 'NRML', 'Validity': 'DAY', 'Price': '10',
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'0','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'202',
                     'StopLossOrderPlacePercent':'250','CallStrikeRequired':True,'PutStrikeRequired':True,'Hedge':False},
                     
                     'Hedge':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'MIDCPNIFTY', 'Quantity': '75', 'Variety': 'AMO', 'Ordertype': 'LIMIT', 'Product': 'NRML', 'Validity': 'DAY', 'Price': '3',
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'0','OptionContractStrikeFromATMPercent':'4','Trigger':'1','StopLossTriggerPercent':'202',
                     'StopLossOrderPlacePercent':'250','CallStrikeRequired':False,'PutStrikeRequired':True,'Hedge':True}}'''
    #print(1)
    print('Waiting to hit the entry time')
    while one_shot_flag == True:
        PrevWkDy = datetime.now().weekday() - 1
        CurrWkDy = datetime.now().weekday()

        now = datetime.now()
        #Condition for entering the specific trade and handling if the entry date is a market holiday
        NiftyStraddle_Mon_12Pm_100Sl =       str(now.strftime("%H:%M:%S")) == '12:00:00' and ((CurrWkDy == MONDAY) or (PrevWkDy == MONDAY and CheckForDateHoliday(PREVIOUSDATE)))
        BankNiftyStraddle_Mon_0930Am_125Sl = str(now.strftime("%H:%M:%S")) == '09:30:00' and ((CurrWkDy == MONDAY  or (PrevWkDy == MONDAY and CheckForDateHoliday(PREVIOUSDATE))))
        NiftyStraddle_Tue_11Am_110Sl =       str(now.strftime("%H:%M:%S")) == '11:00:00' and ((CurrWkDy == TUESDAY)or (PrevWkDy == TUESDAY and CheckForDateHoliday(PREVIOUSDATE)))
        MidCPNiftyStraddle_Wed_13Pm_90Sl =   str(now.strftime("%H:%M:%S")) == '13:00:00' and ((CurrWkDy == WEDNESDAY)or (PrevWkDy == WEDNESDAY and CheckForDateHoliday(PREVIOUSDATE)))
        FINNiftyStraddle_Thu_1430Pm_50Sl =   str(now.strftime("%H:%M:%S")) == '14:30:00' and ((CurrWkDy == THURSDAY) or (PrevWkDy == THURSDAY and CheckForDateHoliday(PREVIOUSDATE)))
        BankNiftyStraddle_Fri_930Am_100Sl =  str(now.strftime("%H:%M:%S")) == '19:30:00' and ((CurrWkDy == FRIDAY)   or (PrevWkDy == FRIDAY and CheckForDateHoliday(PREVIOUSDATE)))

        BankNiftySellCall_Wed_1520Pm_50Sl =  str(now.strftime("%H:%M:%S")) == '15:20:00' and ((CurrWkDy == WEDNESDAY)or (PrevWkDy == WEDNESDAY and CheckForDateHoliday(PREVIOUSDATE)))#((CurrWkDy == WEDNESDAY) or CheckForDateHoliday(PREVIOUSDATE))
        NiftySellCall_Thu_1520Pm_50Sl =      str(now.strftime("%H:%M:%S")) == '15:20:00' and ((CurrWkDy == THURSDAY)or (PrevWkDy == THURSDAY and CheckForDateHoliday(PREVIOUSDATE)))#((CurrWkDy == THURSDAY)  or CheckForDateHoliday(PREVIOUSDATE))

        #For testing
        AngelBankNiftyLongCallMonthlyFirstDayMonExpiry = str(now.strftime("%H:%M:%S")) == '16:20:00' and ((CurrWkDy == WEDNESDAY)or (PrevWkDy == WEDNESDAY and CheckForDateHoliday(PREVIOUSDATE)))


        AngelNararushBankNiftySellCall_Tue_1000Am_100Sl =        str(now.strftime("%H:%M:%S")) == '10:00:00' and ((CurrWkDy == TUESDAY)or (PrevWkDy == TUESDAY and CheckForDateHoliday(PREVIOUSDATE)))
        AngelNararushNiftySellCall_Wed_1000Am_100Sl =            str(now.strftime("%H:%M:%S")) == '10:00:00' and ((CurrWkDy == WEDNESDAY)or (PrevWkDy == WEDNESDAY and CheckForDateHoliday(PREVIOUSDATE)))
        AngelNararushNiftySellPut_Mon_1000Am_100Sl =             str(now.strftime("%H:%M:%S")) == '10:00:00' and ((CurrWkDy == MONDAY)or (PrevWkDy == MONDAY and CheckForDateHoliday(PREVIOUSDATE)))
        AngelNararushBankNiftySellPut_Fri_1000Am_100Sl =         str(now.strftime("%H:%M:%S")) == '10:00:00' and ((CurrWkDy == FRIDAY)or (PrevWkDy == FRIDAY and CheckForDateHoliday(PREVIOUSDATE)))
        
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
        BankNiftyLongCallMonthlyFirstDayMonExpiry = ((date.today() == FirstMonthNiftyCallLongDate) or (CheckForDateHoliday(FirstMonthNiftyCallLongDate) and PREVIOUSDATE == FirstMonthBankNiftyCallLongDate)) and str(now.strftime("%H:%M:%S")) == '09:30:30'
        NiftyLongCallMonthlyFirstDayMonExpiry =     ((date.today() == FirstMonthNiftyCallLongDate )    or (CheckForDateHoliday(FirstMonthNiftyCallLongDate)     and PREVIOUSDATE == FirstMonthNiftyCallLongDate))     and str(now.strftime("%H:%M:%S")) == '09:30:55'

        #AngeNiftySellPut_Mon_1000Am_100Sl = str(now.strftime("%H:%M:%S")) == '10:00:00' and ((CurrWkDy == MONDAY  or (PrevWkDy == MONDAY and CheckForDateHoliday(PREVIOUSDATE))))

        #Sell Nifty Straddle every monday @ 12pm with 100sl
        if NiftyStraddle_Mon_12Pm_100Sl or Override == '1A': 
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'NIFTY', 'Quantity': '50', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'3','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'102',
                     'StopLossOrderPlacePercent':'150','CallStrikeRequired':'True','PutStrikeRequired':'True','Hedge':'False',"OrderTag":"1NF-STR-MO-12-100"},
                     
                     'Hedge':{'Tradetype': 'BUY', 'Exchange': 'NFO', 'Tradingsymbol': 'NIFTY', 'Quantity': '50', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'3','OptionContractStrikeFromATMPercent':'4','Trigger':'1','StopLossTriggerPercent':'102',
                     'StopLossOrderPlacePercent':'150','CallStrikeRequired':'False','PutStrikeRequired':'True','Hedge':'True',"OrderTag":"1NF-STRH-MO-12-100"}}
            one_shot_flag == False
            Override = False
            #print(OrderDetails['Straddle']['Tradingsymbol'])
            break   

        #Sell BN straddle every monday @930 with 100sl
        if BankNiftyStraddle_Mon_0930Am_125Sl or Override == '1B': 
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'BANKNIFTY', 'Quantity': '15', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'2','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'126',
                     'StopLossOrderPlacePercent':'160','CallStrikeRequired':'True','PutStrikeRequired':'True','Hedge':'False',"OrderTag":"2BN-STR-MO-930-125"},
                     
                     'Hedge':{'Tradetype': 'BUY', 'Exchange': 'NFO', 'Tradingsymbol': 'BANKNIFTY', 'Quantity': '15', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'2','OptionContractStrikeFromATMPercent':'4','Trigger':'1','StopLossTriggerPercent':'126',
                     'StopLossOrderPlacePercent':'160','CallStrikeRequired':'False','PutStrikeRequired':'True','Hedge':'True',"OrderTag":"2BN-STRH-MO-930-125"}}
            one_shot_flag == False
            Override = False
            #print(OrderDetails['Straddle']['Tradingsymbol'])
            break  

        #Sell N straddle every Tuesday @11am with 110sl
        if NiftyStraddle_Tue_11Am_110Sl or Override == '2': 
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'NIFTY', 'Quantity': '50', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'3','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'111',
                     'StopLossOrderPlacePercent':'155','CallStrikeRequired':'True','PutStrikeRequired':'True','Hedge':'False',"OrderTag":"3NF-STR-TU-11-110"},
                     
                     'Hedge':{'Tradetype': 'BUY', 'Exchange': 'NFO', 'Tradingsymbol': 'NIFTY', 'Quantity': '50', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'3','OptionContractStrikeFromATMPercent':'4','Trigger':'1','StopLossTriggerPercent':'111',
                     'StopLossOrderPlacePercent':'155','CallStrikeRequired':'False','PutStrikeRequired':'True','Hedge':'True',"OrderTag":"3NF-STRH-TU-11-110"}}
            one_shot_flag == False
            Override = False
            #print(OrderDetails['Straddle']['Tradingsymbol'])
            break  
        
        #Sell MN straddle every Wednesday @1300 with 90sl
        if MidCPNiftyStraddle_Wed_13Pm_90Sl or Override == '3': 
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'MIDCPNIFTY', 'Quantity': '75', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'0','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'92',
                     'StopLossOrderPlacePercent':'140','CallStrikeRequired':'True','PutStrikeRequired':'True','Hedge':'False',"OrderTag":"4MN-STR-WE-13-90"},
                     
                     'Hedge':{'Tradetype': 'BUY', 'Exchange': 'NFO', 'Tradingsymbol': 'MIDCPNIFTY', 'Quantity': '75', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'0','OptionContractStrikeFromATMPercent':'4','Trigger':'1','StopLossTriggerPercent':'92',
                     'StopLossOrderPlacePercent':'140','CallStrikeRequired':'False','PutStrikeRequired':'True','Hedge':'True',"OrderTag":"4MN-STRH-WE-13-90"}}
            one_shot_flag == False
            Override = False
            #print(OrderDetails['Straddle']['Tradingsymbol'])
            break

        #Sell FN straddle every Thursday @1430 with 50sl
        if FINNiftyStraddle_Thu_1430Pm_50Sl or Override == '4': 
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'FINNIFTY', 'Quantity': '40', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'1','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'52',
                     'StopLossOrderPlacePercent':'82','CallStrikeRequired':'True','PutStrikeRequired':'True','Hedge':'False',"OrderTag":"5FN-STR-TH-1430-50"},
                     
                     'Hedge':{'Tradetype': 'BUY', 'Exchange': 'NFO', 'Tradingsymbol': 'FINNIFTY', 'Quantity': '40', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'1','OptionContractStrikeFromATMPercent':'4','Trigger':'1','StopLossTriggerPercent':'52',
                     'StopLossOrderPlacePercent':'82','CallStrikeRequired':'False','PutStrikeRequired':'True','Hedge':'True',"OrderTag":"5FN-STRH-TH-1430-50"}}#_#sltrigpercent
            one_shot_flag == False
            Override = False
            #print(OrderDetails['Straddle']['Tradingsymbol'])
            break 

        #Sell BN straddle every Friday @930 with 100sl
        if BankNiftyStraddle_Fri_930Am_100Sl or Override == '5': 
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'BANKNIFTY', 'Quantity': '15', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'2','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'101',
                     'StopLossOrderPlacePercent':'150','CallStrikeRequired':'True','PutStrikeRequired':'True','Hedge':'False',"OrderTag":"6BN-STR-FR-930-100"},
                     
                     'Hedge':{'Tradetype': 'BUY', 'Exchange': 'NFO', 'Tradingsymbol': 'BANKNIFTY', 'Quantity': '15', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'2','OptionContractStrikeFromATMPercent':'4','Trigger':'1','StopLossTriggerPercent':'101',
                     'StopLossOrderPlacePercent':'150','CallStrikeRequired':'False','PutStrikeRequired':'True','Hedge':'True',"OrderTag":"6BN-STRH-FR-930-100"}}
            one_shot_flag == False
            Override = False
            #print(OrderDetails['Straddle']['Tradingsymbol'])
            break

        #Sell BN Call every Wed @1520 with 50sl
        if BankNiftySellCall_Wed_1520Pm_50Sl or Override == '6': 
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'BANKNIFTY', 'Quantity': '15', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'2','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'52',
                     'StopLossOrderPlacePercent':'92','CallStrikeRequired':'True','PutStrikeRequired':'False','Hedge':'False',"OrderTag":"7BN-SC1-WE-1520-50"}}
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

        #Buy Nifty Call beggining of each month contract
        if NiftyLongCallMonthlyFirstDayMonExpiry or Override == '8': 
            OrderDetails = {'Straddle':{'Tradetype': 'BUY', 'Exchange': 'NFO', 'Tradingsymbol': 'NIFTY', 'Quantity': '50', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'3','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'999',
                     'StopLossOrderPlacePercent':'999','CallStrikeRequired':'True','PutStrikeRequired':'False','Hedge':'MonthlyCall',"OrderTag":"8NF-LC2-FM-930-NOSL"}}
            #one_shot_flag == False
            Override = False
            #print(OrderDetails['Straddle']['Tradingsymbol'])
            break   
        
        #Buy BankNifty Call beggining of each month contract
        if BankNiftyLongCallMonthlyFirstDayMonExpiry or Override == '9': 
            OrderDetails = {'Straddle':{'Tradetype': 'BUY', 'Exchange': 'NFO', 'Tradingsymbol': 'BANKNIFTY', 'Quantity': '15', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'','Netposition':'','OptionExpiryDay':'2','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'999',
                     'StopLossOrderPlacePercent':'999','CallStrikeRequired':'True','PutStrikeRequired':'False','Hedge':'MonthlyCall',"OrderTag":"7BN-LC1-FM-930-NOSL"}}
            #one_shot_flag == False
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

        #Sell BN Call every Tue @1000 with 100sl
        if AngelNararushBankNiftySellCall_Tue_1000Am_100Sl or Override == '17': 
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'BANKNIFTY', 'Quantity': '15', 'Variety': 'NORMAL', 'Ordertype': 'MARKET', 'Product': 'CARRYFORWARD', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ANGEL','Netposition':'','OptionExpiryDay':'2','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'102',
                     'StopLossOrderPlacePercent':'152','CallStrikeRequired':'True','PutStrikeRequired':'False','Hedge':'False',"OrderTag":"","User":"nararush","TimePeriod":"1"}}
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
        
        #Sell BN Put every Fri @1000 with 100sl
        if AngelNararushBankNiftySellPut_Fri_1000Am_100Sl or Override == '15': 
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'BANKNIFTY', 'Quantity': '15', 'Variety': 'NORMAL', 'Ordertype': 'MARKET', 'Product': 'CARRYFORWARD', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ANGEL','Netposition':'','OptionExpiryDay':'2','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'102',
                     'StopLossOrderPlacePercent':'152','CallStrikeRequired':'False','PutStrikeRequired':'True','Hedge':'False',"OrderTag":"","User":"nararush","TimePeriod":"5"}}
            one_shot_flag == False
            Override = False
            #print(OrderDetails['Straddle']['Tradingsymbol'])
            break
        
        ######################################################################################################################################################################################################################################################
        #testing purpose
        #Place Finifty order during active market hour for testing with GTT order set for Kite
        if  Override == '99':
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'FINNIFTY', 'Quantity': '40', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ZERODHA_OPTION','Netposition':'','OptionExpiryDay':'1','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'65',
                     'StopLossOrderPlacePercent':'95','CallStrikeRequired':'True','PutStrikeRequired':'False','Hedge':'False',"OrderTag":"12FN-SC-MACD-WE-65"}}    
            one_shot_flag == False
            Override = False
            #print(OrderDetails['Straddle']['Tradingsymbol'])
            break

        #testing purpose
        #Place Finifty order post market hour for testing with GTT order set for Kite
        if  Override == '98':
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'FINNIFTY', 'Quantity': '40', 'Variety': 'AMO', 'Ordertype': 'LIMIT', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 350,
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
            OrderDetails = {"Tradetype": "BUY", "Exchange": "NCDEX", "Tradingsymbol": "CASTOR20DEC2023", "Quantity": "1*5", "Variety": "AMO", "Ordertype": "LIMIT", "Product": "CARRYFORWARD",
                             "Validity": "DAY", "Price": 5930, "Symboltoken":"CASTOR20DEC2023", "Squareoff":"", "Stoploss":"", "Broker":"ANGEL"}
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



            