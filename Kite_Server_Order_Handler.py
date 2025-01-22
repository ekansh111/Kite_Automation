"""
This script demonstrates how to place orders with Zerodha (Kite API) using Python.
It includes functionality for:
- Reading an instrument details file (CSV) and filtering based on certain criteria (like expiry date).
- Establishing a connection/session to the Kite Connect using user credentials.
- Validating and preparing order details (e.g., setting limit price to LTP if Ordertype != MARKET).
- Placing limit or market orders, with a potential fallback to convert unfilled limit orders to market orders.
- Handling contract rollover logic for futures based on a specified RolloverDate.
"""

# package import statement
import time
from datetime import date, datetime, timedelta
import pandas as pd
import pytz
from Directories import *
from Fetch_Positions_Data import get_order_status
from Server_Order_Place import *

# For Kite Connect
from kiteconnect import KiteConnect

# Types of Orders
LimitOrder = 'LIMIT'
MarketOrder = 'MARKET'

def PrepareInstrumentContractNameKite(kite,OrderDetails):
    """
    This function calls the respective instrument contract preparation function.
    """

    ZerodhaInstrument_filtered = PrepareKiteInstrumentContractName(kite,OrderDetails)

    UpdateRequestContractDetailsKite(OrderDetails, ZerodhaInstrument_filtered)

    return OrderDetails

def PrepareKiteInstrumentContractName(kite,OrderDetails):
    """
    Reads the instrument details from a CSV (ZerodhaInstrumentDirectory),
    applies filtering logic based on OrderDetails, and returns the filtered DataFrame.
    """
    
    # Read the CSV file into a DataFrame
    ZerodhaInstrumentDetails = pd.read_csv(ZerodhaInstrumentDirectory, delimiter=',')
    # If there's an unnamed column (index) we rename it:
    ZerodhaInstrumentDetails.rename(columns={'Unnamed: 0': 'serialnumber'}, inplace=True)

    # Current datetime for reference
    today = datetime.now()

    # Compute the rollover date by adding 'DaysPostWhichSelectNextContract' to today's date
    RolloverDate = today + timedelta(days=int(OrderDetails['DaysPostWhichSelectNextContract']))

    # Convert the 'expiry' column to a datetime. e.g., "28FEB2025" => datetime object
    ZerodhaInstrumentDetails['expiry'] = pd.to_datetime(
        ZerodhaInstrumentDetails['expiry'].str.title(), 
        format='%Y-%m-%d', #Important, the date format may be subject to change
        errors='coerce'
    )
    
    ZerodhaInstrumentDetails_filtered = pd.DataFrame()

    if int(OrderDetails['Netposition']) == 0:
        ZerodhaInstrumentDetails_filtered = CheckIfExistingOldContractSqOffReq(kite,ZerodhaInstrumentDetails,OrderDetails,today,RolloverDate)
    
    if ZerodhaInstrumentDetails_filtered.empty:
        ZerodhaInstrumentDetails_filtered = ZerodhaInstrumentDetails[
            (ZerodhaInstrumentDetails['name'] == OrderDetails['Tradingsymbol']) &
            (ZerodhaInstrumentDetails['exch_seg'] == OrderDetails['Exchange']) &
            (ZerodhaInstrumentDetails['instrumenttype'] == OrderDetails['InstrumentType']) &
            (ZerodhaInstrumentDetails['expiry'] > RolloverDate)
        ].sort_values(by='expiry', ascending=True).head(1)
    
    return ZerodhaInstrumentDetails_filtered

def CheckIfExistingOldContractSqOffReq(kite, ZerodhaInstrumentDetails, OrderDetails, today, RolloverDate):
    """
    This function checks if an existing old futures or options contract requires squaring off before rollover.
    It filters the available contracts based on the provided criteria (such as expiry date and symbol)
    and manages the order flow if a matching position exists.

    Args:
        kite: The Kite API instance used for accessing market data and managing positions.
        ZerodhaInstrumentDetails: DataFrame containing details of all available contracts.
        OrderDetails: Dictionary containing details of the order, such as symbol, exchange, and instrument type.
        today: The current date (used to filter contracts that are not expired).
        RolloverDate: The cutoff date (used to identify contracts that need to be squared off before this date).

    Returns:
        - A filtered DataFrame (`KitePositionsFiltered`) with matching positions if an old contract needs squaring off.
        - An empty DataFrame if no matching old contract exists.
    """
    # Step 1: Filter the contracts based on the given criteria
    # Match the symbol, exchange, and instrument type, and filter by expiry date range.
    ZerodhaInstrumentDetails_filtered = ZerodhaInstrumentDetails[
        (ZerodhaInstrumentDetails['name'] == OrderDetails['Tradingsymbol']) &  # Match the trading symbol
        (ZerodhaInstrumentDetails['exch_seg'] == OrderDetails['Exchange']) &  # Match the exchange segment
        (ZerodhaInstrumentDetails['instrumenttype'] == OrderDetails['InstrumentType']) &  # Match the instrument type
        (ZerodhaInstrumentDetails['expiry'] >= today) &  # Ensure the contract has not expired
        (ZerodhaInstrumentDetails['expiry'] <= RolloverDate)  # Ensure the contract is within the rollover period
    ].sort_values(by='expiry', ascending=True).head(1)  # Sort by expiry and pick the earliest

    # Step 2: Check if any matching contract exists
    if not ZerodhaInstrumentDetails_filtered.empty:
        # Fetch existing positions from Kite for the given order details
        KitePositions = FetchExistingNetKitePositions(kite, OrderDetails)

        # Further filter the Kite positions to match the selected contract's symbol and token
        KitePositionsFiltered = KitePositions[
            (KitePositions['tradingsymbol'] == ZerodhaInstrumentDetails_filtered['symbol'].iloc[0]) &  # Match the trading symbol
            (KitePositions['instrument_token'] == ZerodhaInstrumentDetails_filtered['token'].iloc[0])  # Match the instrument token
        ].copy()

        # Rename columns in the copied DataFrame
        KitePositionsFiltered.rename(columns={'tradingsymbol': 'symbol', 'instrument_token': 'token'}, inplace=True)


        # Step 3: If there are matching positions, return the filtered positions
        if not KitePositions.empty:
            # Rename columns to standardize naming for further processing
            KitePositionsFiltered.rename(columns={'tradingsymbol': 'symbol', 'instrument_token': 'token'}, inplace=True)

            # Return the filtered positions DataFrame
            return KitePositionsFiltered
    else:
        # If no matching contract is found, return an empty DataFrame
        return pd.DataFrame()  # Ensure an empty DataFrame is returned for consistency



def FetchExistingNetKitePositions(kite,OrderDetails):
        
        #Fetch positions from kite account
        positions = kite.positions()
        
        # Extract net positions
        net_positions = positions['net']
        ZerodhaInstrument_positions = pd.DataFrame(net_positions)

        return ZerodhaInstrument_positions
    
def UpdateRequestContractDetailsKite(OrderDetails, ZerodhaInstrument_filtered):
    """
    Updates the OrderDetails dictionary with the new contract
    (symbol and token) from the filtered DataFrame.
    """

    # Retrieve the first row's symbol and token values
    OrderDetails['Tradingsymbol'] = ZerodhaInstrument_filtered['symbol'].iloc[0]
    OrderDetails['Symboltoken']   = ZerodhaInstrument_filtered['token'].iloc[0]

    return OrderDetails


def EstablishConnectionKiteAPI(OrderDetails):
    """
    Reads credentials from a specified file and creates a Kite Connect session.
    Typically:
      Line 1: api_key
      Line 2: request_token
      Line 3: api_secret
    We then exchange the request_token for an access_token.
    Once the access_token is acquired, we set it on the KiteConnect instance.
    """
    
    if str(OrderDetails.get('User')) == 'IK6635':
        APIKeyDirectory = KiteEkanshLogin
        AccessTokenDirectory = KiteEkanshLoginAccessToken

    elif str(OrderDetails.get('User')) == 'YD6016':  
        APIKeyDirectory = KiteRashmiLogin
        AccessTokenDirectory = KiteRashmiLoginAccessToken
    
    with open(APIKeyDirectory,'r') as InputsFile:
        content = InputsFile.readlines()
        api_key   = content[2].strip('\n')
        InputsFile.close()

    kite = KiteConnect(api_key=api_key)

    with open(AccessTokenDirectory,'r') as f:
        access_tok = f.read()
        f.close()

    kite.set_access_token(access_tok)
    return kite


def Validate_Quantity(OrderDetails):
    """
    If quantity is given in a multiplier format like "2*50", 
    parse and multiply to get the final integer quantity.
    """
    Quantitysplit = str(OrderDetails['Quantity']).split('*')

    if len(Quantitysplit) > 1:
        UpdatedQuantity = int(Quantitysplit[0]) * int(Quantitysplit[1])
        OrderDetails['Quantity'] = UpdatedQuantity 
        print("Updated quantity:", UpdatedQuantity)
    
    return OrderDetails


def PlaceOrderKiteAPI(kite, OrderDetails):
    """
    Places the order using the Kite Connect API. 
    Adjust parameters (variety, product, etc.) as needed.
    """
    print('Order details in PlaceOrderKiteAPI:')
    print(OrderDetails)
    
    order_id = order(OrderDetails)

    return order_id

def ConvertToMarketOrder(kite, OrderDetails):
    """
    Convert the existing order details to a market order 
    and place it again if the limit order didn't fill.
    """
    OrderDetails['Price']     = 0.0
    OrderDetails['Ordertype'] = MarketOrder
    return PlaceOrderKiteAPI(kite, OrderDetails)


def SleepForRequiredTime(SleepTime):
    """
    Pause execution for a specified time in seconds.
    """
    time.sleep(SleepTime)
    return True


def PrepareOrderKite(kite, OrderDetails):
    """
    Retrieves the LTP data for the instrument to set the Limit price
    if the Ordertype is not MARKET.
    """
    # Note: Zerodha’s LTP endpoint is typically `kite.ltp()` which takes a list of instrument tokens.
    # The key used is "NSE:RELIANCE" or "NFO:BANKNIFTY23OCTFUT" etc. 
    # You must build the exchange-tradingsymbol string for the LTP call.

    exchange_symbol = f"{OrderDetails['Exchange']}:{OrderDetails['Tradingsymbol']}"
    try:
        ltp_data = kite.ltp([exchange_symbol])  # Returns a dict
        instrument_ltp = ltp_data[exchange_symbol]['last_price']
        print("LTP Info:", instrument_ltp)

        if OrderDetails['Ordertype'] != 'MARKET':
            OrderDetails['Price'] = instrument_ltp
    except Exception as e:
        print("Error fetching LTP data:", e)

    return OrderDetails


def ControlOrderFlowKite(OrderDetails):
    """
    Orchestrates the entire order flow for Kite, from
    contract selection to final order placement (Limit, fallback to Market).
    """

    # Create a Kite Connect session
    kite = EstablishConnectionKiteAPI(OrderDetails)

    # If the contract name is not directly provided, figure it out
    if OrderDetails['ContractNameProvided'] == 'False':
        PrepareInstrumentContractNameKite(kite,OrderDetails)

    # Validate and fix quantity if needed
    Validate_Quantity(OrderDetails)

    # Optionally fetch LTP and set the limit price if not a market order
    OrderDetails = PrepareOrderKite(kite, OrderDetails)

    # Place the (possibly) limit order
    order_id = PlaceOrderKiteAPI(kite, OrderDetails)

    # If it’s a MARKET order, we’re done
    if OrderDetails['Ordertype'].upper() == 'MARKET':
        return order_id
    else:
        order_list = []
        order_list.append(order_id)
        print(order_list)
        if OrderDetails['ConvertToMarketOrder'] == 'True':
            if int(OrderDetails['Netposition']) != 0:
                print(f'Waiting for {OrderDetails["EntrySleepDuration"]} seconds')
                #Sleep for the designated time
                SleepForRequiredTime(int(OrderDetails['EntrySleepDuration']))
            else:
                print(f'Waiting for {OrderDetails["ExitSleepDuration"]} seconds')
                #Sleep for the designated time
                SleepForRequiredTime(int(OrderDetails['ExitSleepDuration']))
            OrderType = 'MARKET'
            ReorderFlag = 1

            get_order_status(kite, order_list, OrderType, ReorderFlag)    

        return order_id
