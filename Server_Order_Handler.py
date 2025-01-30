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
import json

#Types of Orders
LimitOrder = 'LIMIT'
MarketOrder = 'MARKET'

def ConfigureNetDirectionOfTrade(OrderDetails):
    if OrderDetails['Tradetype'].strip().upper() == 'BUY':
        OrderDetails['NetDirection'] = 1
    elif OrderDetails['Tradetype'].strip().upper() == 'SELL':
        OrderDetails['NetDirection'] = -1
    return OrderDetails

def PrepareInstrumentContractName(smartAPI, OrderDetails):
    """
    This function determines the broker from the OrderDetails and
    calls the respective instrument contract preparation function.
    """
    
    # Check broker type in the order details
    if OrderDetails['Broker'] == 'ANGEL':
        # If broker is Angel, prepare instrument contract for Angel
        AngelInstrument_filtered = PrepareAngelInstrumentContractName(smartAPI,OrderDetails)    
        
        UpdateRequestContractDetailsAngel(OrderDetails, AngelInstrument_filtered)

        return OrderDetails


def PrepareAngelInstrumentContractName(smartAPI,OrderDetails):
    """
    Reads the instrument details from AngelInstrumentDirectory CSV,
    applies filtering logic based on OrderDetails, and returns
    the filtered DataFrame.
    """

    # Read the CSV file into a DataFrame
    AngelInstrumentDetails = pd.read_csv(AngelInstrumentDirectory, delimiter=',')
    # The CSV might have an unnamed first column which we rename below

    # Rename only the unnamed column to 'serialnumber' if it exists
    AngelInstrumentDetails.rename(columns={'Unnamed: 0': 'serialnumber'}, inplace=True)
    
    # Current datetime for reference
    today = datetime.now()

    # Compute the rollover date by adding 'DaysPostWhichSelectNextContract'
    # to today's date
    RolloverDate = today + timedelta(days=int(OrderDetails['DaysPostWhichSelectNextContract']))

    # Convert the 'expiry' column to a proper datetime format.
    # Example expiry string: "28FEB2025" => datetime object
    AngelInstrumentDetails['expiry'] = pd.to_datetime(AngelInstrumentDetails['expiry'].str.title(), format='%d%b%Y', errors='coerce')

    AngelInstrumentDetails_filtered = pd.DataFrame()

    # If Netposition == '0', filter by expiry > today and pick the nearest expiry 
    if ((int(OrderDetails['Netposition']) != int(OrderDetails['Quantity'])) or (OrderDetails.get('ReEnterOrderLoop') == 'True')):

        if int(OrderDetails['Netposition']) == 0:
            AngelInstrumentDetails_filtered = CheckIfExistingOldContractSqOffReqAngel(smartAPI,AngelInstrumentDetails,OrderDetails,today,RolloverDate)

        else:
            if OrderDetails.get('ReEnterOrderLoop') == 'True':
                OrderDetails['Quantity'] = OrderDetails['QuantityToBePlacedInNextRound']
                OrderDetails['ReEnterOrderLoop'] == 'False'
                OrderDetails['Tradingsymbol'] = OrderDetails['InitialTradingsymbol']
                
            
            else:
                OrderDetails['InitialTradingsymbol'] = OrderDetails['Tradingsymbol']

                AngelInstrumentDetails_filtered = CheckIfExistingOldContractSqOffReqAngel(smartAPI,AngelInstrumentDetails,OrderDetails,today,RolloverDate)
                if not AngelInstrumentDetails_filtered.empty:
                    OrderDetails['ReEnterOrderLoop'] = 'True'

                    NoOfContractsInOldMonthFormat = int(AngelInstrumentDetails_filtered['netqty'].iloc[0])
                    NoOfContractsInNewMonthFormatToPlaceOrders = int(OrderDetails['Quantity']) 

                    if NoOfContractsInNewMonthFormatToPlaceOrders > NoOfContractsInOldMonthFormat:
                        InitialOrderQuantity = NoOfContractsInOldMonthFormat#NoOfContractsInNewMonthFormatToPlaceOrders
                        NetQuantityOrdersToBePlaced = NoOfContractsInNewMonthFormatToPlaceOrders - abs(NoOfContractsInOldMonthFormat)

                    else:
                        InitialOrderQuantity = NoOfContractsInNewMonthFormatToPlaceOrders
                        NetQuantityOrdersToBePlaced = NoOfContractsInOldMonthFormat - abs(NoOfContractsInNewMonthFormatToPlaceOrders)

                    if InitialOrderQuantity < 0:
                        InitialOrderQuantity = InitialOrderQuantity * -1
                        
                    OrderDetails['Quantity'] = InitialOrderQuantity
                    OrderDetails['QuantityToBePlacedInNextRound'] = NetQuantityOrdersToBePlaced




    if AngelInstrumentDetails_filtered.empty:
        AngelInstrumentDetails_filtered = AngelInstrumentDetails[
            (AngelInstrumentDetails['name'] == OrderDetails['Tradingsymbol']) &
            (AngelInstrumentDetails['exch_seg'] == OrderDetails['Exchange']) &
            (AngelInstrumentDetails['instrumenttype'] == OrderDetails['InstrumentType']) &
            (AngelInstrumentDetails['expiry'] > RolloverDate)
        ].sort_values(by='expiry', ascending=True).head(1)
    
    return AngelInstrumentDetails_filtered

def CheckIfExistingOldContractSqOffReqAngel(smartAPI, AngelInstrumentDetails, OrderDetails, today, RolloverDate):
    """
    Checks if there's an old contract that requires square-off in the specified date range.
    Filters the instrument details based on the OrderDetails, then compares it against
    existing Angel positions to see if there's a matching position to square off.
    
    :param smartAPI:      The authenticated Angel One (SmartAPI) session object.
    :param AngelInstrumentDetails: A DataFrame containing instrument details (symbol, token, expiry, etc.).
    :param OrderDetails:  A dictionary with order-related details (Tradingsymbol, Exchange, InstrumentType, etc.).
    :param today:         The current date/time (datetime object).
    :param RolloverDate:  The rollover deadline date/time (datetime object).
    :return:              A filtered DataFrame of positions matching the old contract criteria. 
                          Returns an empty DataFrame if none match.
    """
    
    # Step 1: Filter the contracts based on the given criteria
    # Match the symbol, exchange, and instrument type, and filter by expiry date range.
    AngelInstrumentDetails_filtered = AngelInstrumentDetails[
        (AngelInstrumentDetails['name'] == OrderDetails['Tradingsymbol']) &  # Match the trading symbol
        (AngelInstrumentDetails['exch_seg'] == OrderDetails['Exchange']) &  # Match the exchange segment
        (AngelInstrumentDetails['instrumenttype'] == OrderDetails['InstrumentType']) &  # Match the instrument type
        (AngelInstrumentDetails['expiry'] >= today) &  # Ensure the contract has not expired
        (AngelInstrumentDetails['expiry'] <= RolloverDate)  # Ensure the contract is within the rollover period
    ].sort_values(by='expiry', ascending=True).head(1)  # Sort by expiry and pick the earliest

    # Step 2: Check if any matching contract exists
    if not AngelInstrumentDetails_filtered.empty:
        # Fetch existing positions from Angel for the given order details
        AngelPositionsDetails = FetchExistingAngelPositions(smartAPI, OrderDetails)
        AngelPositions = pd.DataFrame(AngelPositionsDetails)

        AngelPositionsData = pd.DataFrame(AngelPositions['data'].tolist())

        AngelPositionsData['netqty'] = pd.to_numeric(AngelPositionsData['netqty'], errors='coerce')

        # 1. Determine the comparison condition based on Tradetype
        if str(OrderDetails['Tradetype']).upper() == 'BUY':
            comparison_condition = (AngelPositionsData['netqty'] < OrderDetails['NetDirection'])
        else:
            comparison_condition = (AngelPositionsData['netqty'] > OrderDetails['NetDirection'])

        # 2. Apply the condition in the DataFrame filter
        AngelPositionsFiltered = AngelPositionsData[
            (AngelPositionsData['symboltoken'] == AngelInstrumentDetails_filtered['token'].iloc[0]) &
            (AngelPositionsData['netqty'] != 0) &
            comparison_condition
        ].copy()


        # Step 3: If there are matching positions, return the filtered positions
        if not AngelPositionsFiltered.empty:
            # Rename columns to standardize naming for further processing
            AngelPositionsFiltered.rename(columns={'symbol': 'instrument_name', 'tradingsymbol': 'symbol', 'instrument_token': 'token', 'symboltoken': 'token'}, inplace=True)
            # Return the filtered positions DataFrame
            return AngelPositionsFiltered
        else:
            return pd.DataFrame()     
    else:
        # If no matching contract is found, return an empty DataFrame
        return pd.DataFrame()  # Ensure an empty DataFrame is returned for consistency


def FetchExistingAngelPositions(smartAPI, OrderDetails):
    """
    Fetches the user's existing positions from the Angel One (SmartAPI).
    
    :param smartAPI:     The authenticated Angel One (SmartAPI) session object.
    :param OrderDetails: A dictionary containing order-related details (not used in this function directly).
    :return:             A pandas DataFrame containing all current positions.
    """
    # The 'position()' method returns a list/dict of positions. We convert to a DataFrame for easier handling
    positions = smartAPI.position()
    AngelInstrument_positions = pd.DataFrame(positions)

    return AngelInstrument_positions


def UpdateRequestContractDetailsAngel(OrderDetails, AngelInstrument_filtered):
    """
    Updates the OrderDetails dictionary with the new contract
    (symbol and token) from the filtered DataFrame.
    """
    
    # Retrieve the first row's symbol and token values
    OrderDetails['Tradingsymbol'] = AngelInstrument_filtered['symbol'].iloc[0]
    OrderDetails['Symboltoken'] = AngelInstrument_filtered['token'].iloc[0]

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
        UpdatedNetQuantity = int(OrderDetails['Netposition']) * int(Quantitysplit[1])
        
        OrderDetails['Quantity'] = UpdatedQuantity 
        OrderDetails['Netposition'] = UpdatedNetQuantity 
        
    
    return OrderDetails

#Function to place order on Angel Broking account
def PlaceOrderAngelAPI(smartApi,OrderDetails):
    print('Order details in place order')
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
        print("Order placement failed: {}".format(str(e)))

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


def ModifyAngeOrder(smartAPI, OrderDetails):
    """
    Modifies an existing Angel One order by sending updated parameters
    to the SmartAPI modifyOrder endpoint.

    :param smartAPI:      The authenticated SmartAPI session object.
    :param OrderDetails:  A dictionary containing the details needed to modify the order.
                          Must include:
                           - Variety (e.g., "NORMAL", "STOPLOSS")
                           - OrderId (the existing order ID to modify)
                           - Tradingsymbol (symbol name used in the original order)
                           - Symboltoken (token for the symbol)
                           - Tradetype ("BUY" or "SELL")
                           - Exchange (e.g., "MCX")
                           - Ordertype ("MARKET", "LIMIT", "SL", etc.)
                           - Product (e.g., "CARRYFORWARD")
                           - Validity ("DAY", "IOC", etc.)
                           - Quantity (desired quantity to modify)
                           - Price (0 for market or limit price if needed)
    """

    # Prepare the parameters for the modifyOrder API call
    ModifyOrderParams = {
        "variety":         OrderDetails['Variety'],
        "orderid":         OrderDetails['OrderId'],     # The existing order ID
        "tradingsymbol":   OrderDetails['Tradingsymbol'],
        "symboltoken":     OrderDetails['Symboltoken'],
        "transactiontype": OrderDetails['Tradetype'],   # "BUY" or "SELL"
        "exchange":        OrderDetails['Exchange'],    # e.g., "MCX"
        "ordertype":       OrderDetails['Ordertype'],   # e.g., "MARKET", "LIMIT"
        "producttype":     OrderDetails['Product'],     # e.g., "CARRYFORWARD"
        "duration":        OrderDetails['Validity'],    # "DAY", "IOC", etc.
        "quantity":        OrderDetails['Quantity'],    # The updated order quantity
        "price":           OrderDetails['Price']        # 0 if MARKET order, else limit price
    }

    # Send the modify request to the API
    response = smartAPI.modifyOrder(ModifyOrderParams)

    # Print the response to see if the modification succeeded or failed
    print(response)


def ControlOrderFlowAngel(OrderDetails):
    # This function orchestrates the entire order flow for Angel, from contract selection to order placement
    
    smartAPI = EstablishConnectionAngelAPI(OrderDetails)

    ConfigureNetDirectionOfTrade(OrderDetails)

    Validate_Quantity(OrderDetails)

    if OrderDetails['ContractNameProvided'] == 'False':
        PrepareInstrumentContractName(smartAPI,OrderDetails)


    OrderDetails = PrepareOrderAngel(smartAPI, OrderDetails)

    OrderIdDetails = PlaceOrderAngelAPI(smartAPI, OrderDetails)

    OrderDetails['OrderId'] = OrderIdDetails

    #IF few orders remain to be placed due to difference in contract name, then reenter the loop with updated quantity

    if OrderDetails['Ordertype'] == 'MARKET':
        if OrderDetails.get('ReEnterOrderLoop') == 'True':
            PrepareInstrumentContractName(smartAPI,OrderDetails)
            
            OrderDetails = PrepareOrderAngel(smartAPI, OrderDetails)
            OrderIdDetails = PlaceOrderAngelAPI(smartAPI, OrderDetails)
            OrderDetails['OrderId'] = OrderIdDetails
            return OrderDetails  
        return OrderIdDetails
    else:
        if OrderDetails['ConvertToMarketOrder'] == 'True':
            if int(OrderDetails['Netposition']) != 0:
                print(f'Waiting for {OrderDetails["EntrySleepDuration"]} seconds')
                #Sleep for the designated time
                SleepForRequiredTime(int(OrderDetails['EntrySleepDuration']))
            else:
                print(f'Waiting for {OrderDetails["ExitSleepDuration"]} seconds')
                #Sleep for the designated time
                SleepForRequiredTime(int(OrderDetails['ExitSleepDuration']))
            
            OrderDetails['Ordertype'] = 'MARKET'
            OrderDetails['Price'] = '0'
            ModifyAngeOrder(smartAPI,OrderDetails)

            if OrderDetails.get('ReEnterOrderLoop') == 'True':
                OrderDetails['Ordertype'] = 'LIMIT'
                PrepareInstrumentContractName(smartAPI, OrderDetails)                
                OrderDetails = PrepareOrderAngel(smartAPI, OrderDetails)
                OrderIdDetails = PlaceOrderAngelAPI(smartAPI, OrderDetails)
                OrderDetails['OrderId'] = OrderIdDetails
                
                OrderDetails['Ordertype'] = 'MARKET'
                print(f'Waiting for {OrderDetails["EntrySleepDuration"]} seconds')
                SleepForRequiredTime(int(OrderDetails['EntrySleepDuration']))
                ModifyAngeOrder(smartAPI,OrderDetails)
                
                return OrderDetails  
        return OrderIdDetails

