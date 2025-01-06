"""
This script demonstrates how to place orders with the Angel One (SmartAPI) using Python.
It includes functionality for:
- Reading an instrument details file (CSV) and filtering based on certain criteria (like expiry date).
- Establishing a connection/session to the Angel One API using user credentials.
- Validating and preparing order details (e.g., setting limit price to LTP if Ordertype != MARKET).
- Placing limit or market orders, with a potential fallback to convert unfilled limit orders to market orders.
- Handling contract rollover logic for futures based on a specified RolloverDate.
"""
# package import statement
from SmartApi import SmartConnect
import SmartApi
import pyotp
import time
from datetime import date, datetime
import calendar
import pytz
from Directories import *
import pandas as pd
from Directories import *
from datetime import datetime,timedelta

#Types of Orders
LimitOrder = 'LIMIT'
MarketOrder = 'MARKET'

def PrepareInstrumentContractName(OrderDetails):
    """
    This function determines the broker from the OrderDetails and
    calls the respective instrument contract preparation function.
    """
    
    # Check broker type in the order details
    if OrderDetails['Broker'] == 'ANGEL':
        # If broker is Angel, prepare instrument contract for Angel
        print('Broker type angel ')
        df_filtered = PrepareAngelInstrumentContractName(OrderDetails) 
        print(df_filtered)      
        UpdateRequestContractDetailsAngel(OrderDetails, df_filtered)
        print(OrderDetails)

        return OrderDetails

    # If there's another broker like Zerodha, you could add it here
    # elif OrderDetails['Broker'] == 'Zerodha':
    #     PrepareZerodhaInstrumentContractName(OrderDetails)


def PrepareAngelInstrumentContractName(OrderDetails):
    """
    Reads the instrument details from AngelInstrumentDirectory CSV,
    applies filtering logic based on OrderDetails, and returns
    the filtered DataFrame.
    """
    
    # Read the CSV file into a DataFrame
    df = pd.read_csv(AngelInstrumentDirectory, delimiter=',')
    # The CSV might have an unnamed first column which we rename below

    # Rename only the unnamed column to 'serialnumber' if it exists
    df.rename(columns={'Unnamed: 0': 'serialnumber'}, inplace=True)
    
    # Current datetime for reference
    today = datetime.now()

    # Compute the rollover date by adding 'DaysPostWhichSelectNextContract'
    # to today's date
    RolloverDate = today + timedelta(days=int(OrderDetails['DaysPostWhichSelectNextContract']))

    # Convert the 'expiry' column to a proper datetime format.
    # Example expiry string: "28FEB2025" => datetime object
    df['expiry'] = pd.to_datetime(df['expiry'].str.title(), format='%d%b%Y', errors='coerce')

    # If Netposition == '0', filter by expiry > today and pick the nearest expiry
    if int(OrderDetails['Netposition']) == int('0'):
        df_filtered = df[
            (df['name'] == OrderDetails['Tradingsymbol']) &
            (df['exch_seg'] == OrderDetails['Exchange']) &
            (df['instrumenttype'] == OrderDetails['InstrumentType']) &
            (df['expiry'] > today)
        ].sort_values(by='expiry', ascending=True).head(1)
    
    # Otherwise, filter by expiry > RolloverDate (a future date) and pick the nearest expiry
    else:
        print('expiry > rollover date Condition')
        df_filtered = df[
            (df['name'] == OrderDetails['Tradingsymbol']) &
            (df['exch_seg'] == OrderDetails['Exchange']) &
            (df['instrumenttype'] == OrderDetails['InstrumentType']) &
            (df['expiry'] > RolloverDate)
        ].sort_values(by='expiry', ascending=True).head(1)
    
    # Return the filtered DataFrame (top row after sorting by expiry)
    return df_filtered


def UpdateRequestContractDetailsAngel(OrderDetails, df_filtered):
    """
    Updates the OrderDetails dictionary with the new contract
    (symbol and token) from the filtered DataFrame.
    """
    
    # Retrieve the first row's symbol and token values
    OrderDetails['Tradingsymbol'] = df_filtered['symbol'].iloc[0]
    OrderDetails['Symboltoken'] = df_filtered['token'].iloc[0]

    return OrderDetails


#Function to establish a connection with the API
def EstablishConnectionAngelAPI(OrderDetails):
    # This function reads credentials from the specified file and generates a session for Angel API
    
    if str(OrderDetails.get('User')) == 'R71302':
        Directory = AngelNararushLoginCred 
    elif str(OrderDetails.get('User')) == 'E51339915':  
        Directory = AngelEkanshLoginCred           
    
    # Open the credentials file and read all lines
    with open(Directory,'r') as a:
        content = a.readlines()
        a.close() 
    api_key = content[0].strip('\n')
    clientId = content[1].strip('\n')
    pwd = content[2].strip('\n')
    smartApi = SmartConnect(api_key)
    token = content[3].strip('\n')
    totp=pyotp.TOTP(token).now()

    # login api call
    data = smartApi.generateSession(clientId, pwd, totp)

    # print(data)
    authToken = data['data']['jwtToken']
    refreshToken = data['data']['refreshToken']

    # fetch the feedtoken
    feedToken = smartApi.getfeedToken()

    # fetch User Profile
    res = smartApi.getProfile(refreshToken)
    smartApi.generateToken(refreshToken)
    res=res['data']['exchanges']

    return smartApi

#Function to handle disreparency in quantity and lotsizes for order to be placed
def Validate_Quantity(OrderDetails):
    # This function adjusts the quantity if it's given in a multiplier format like "2*50"
    
    Quantitysplit = str(OrderDetails['Quantity']).split('*')

    #If there is any disreparency between the total quantity and lotsize then correct it
    if len(Quantitysplit)>1:
        UpdatedQuantity = int(Quantitysplit[0]) * int(Quantitysplit[1])
        OrderDetails['Quantity'] = UpdatedQuantity 
        print(UpdatedQuantity)
    
    return OrderDetails

#Function to place order on Angel Broking account
def PlaceOrderAngelAPI(smartApi,OrderDetails):
    print('Orer details in place order')
    print(OrderDetails)
    #place order
    try:
        # Prepare the request parameters for placing the order through the Angel API
        orderparams = {
            "variety":str(OrderDetails['Variety']),#Kind of order AMO/NORMAL ...
            "tradingsymbol":str(OrderDetails['Tradingsymbol']).replace(" ","").upper(),#The intrument name
            "symboltoken":str(OrderDetails['Symboltoken']),#Symbol token
            "transactiontype":str(OrderDetails['Tradetype']).upper(),#Buy/Sell
            "exchange":str(OrderDetails['Exchange']),#Exchange to place the order on
            "ordertype":str(OrderDetails['Ordertype']),#LIMIT/MARKET.. Order
            "producttype":str(OrderDetails['Product']),#CARRYFORWARD for futures
            "duration":str(OrderDetails['Validity']),#DAY
            "price":str(OrderDetails['Price']) or "0",
            "squareoff":str(OrderDetails['Squareoff']) or "0",
            "stoploss":str(OrderDetails['Stoploss']) or "0",
            "quantity":str(OrderDetails['Quantity'])#Quantity according to angel one multiplier set
            }
        
        OrderIdDetails = smartApi.placeOrder(orderparams)
    except Exception as e:
        print("Order placement failed: {}".format(e.message))

    return OrderIdDetails

#Function to place market order if the limit order failed
def ConvertToMarketOrder(smartApi,OrderDetails):
    # Converts the existing order details to a market order by setting price=0 and ordertype=MARKET
    
    OrderDetails['Price'] = '0'
    OrderDetails['Ordertype'] = MarketOrder

    PlaceOrderAngelAPI(smartApi,OrderDetails)


def SleepForRequiredTime(SleepTime):
    # Simple utility function to pause execution for a specified time in seconds
    time.sleep(SleepTime)
    return True

#Function to place Limit order first then if not filled , re-place Market Order
def PrepareOrderAngel(smartApi,OrderDetails):
    # This function checks the current LTP and uses it to set the limit order price if needed
    
    exchange = str(OrderDetails['Exchange'])
    tradingsymbol = str(OrderDetails['Tradingsymbol'])
    symboltoken = str(OrderDetails['Symboltoken'])

    LtpInfo = smartApi.ltpData(exchange=exchange,tradingsymbol=tradingsymbol,symboltoken=symboltoken)
    
    Instrumentdata = LtpInfo['data']
    print('LTP Info')
    print(LtpInfo)

    # If ordertype is not MARKET, set the limit price to the latest LTP
    if OrderDetails['Ordertype'] != 'MARKET':
        OrderDetails['Price'] = Instrumentdata['ltp']

    return OrderDetails


#def CheckOrderStatus(smartAPI, OrderIdDetails, OrderDetails):

def ControlOrderFlowAngel(OrderDetails):
    # This function orchestrates the entire order flow for Angel, from contract selection to order placement
    
    if OrderDetails['ContractNameProvided'] == 'False':
        print('Inside Manual order naming')
        PrepareInstrumentContractName(OrderDetails)

    smartAPI = EstablishConnectionAngelAPI(OrderDetails)

    Validate_Quantity(OrderDetails)

    OrderDetails = PrepareOrderAngel(smartAPI, OrderDetails)

    OrderIdDetails = PlaceOrderAngelAPI(smartAPI, OrderDetails)
    
    if OrderDetails['Ordertype'] == 'MARKET':
        return 1
    else:
        print('Limit order placed')
        return 1
        exit(1)
        #SleepForRequiredTime(OrderDetails['SleepDuration'])

    #CheckOrderStatus()

    ConvertToMarketOrder(OrderDetails)
