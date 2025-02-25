"""
This script is designed to automate the placement of intraday orders based on stock data.

**Main Functionalities:**

1. **Order Placement Automation**:
   - Reads stock data from a pandas DataFrame.
   - Selects stocks based on criteria such as lowest and highest open prices.
   - Calculates the quantity to trade based on the open price and capital risked per trade.
   - Prepares order details for both long and short positions.
   - Executes orders using the `order` function from the `Server_Order_Place` module.

2. **Configuration and Validation**:
   - Configures logging to capture important events and errors.
   - Validates the input DataFrame to ensure it contains required columns.

3. **Stock Selection**:
   - Selects the top N stocks with the lowest open prices.
   - Selects the bottom N stocks with the highest open prices.

4. **Order Preparation**:
   - Prepares order details for long (buy) and short (sell) trades.
   - Calculates the quantity of shares to trade based on risk per trade and open price.

5. **Order Execution**:
   - Executes the prepared orders by calling the `order` function.

6. **Logging and Output**:
   - Logs important steps and any errors that occur during execution.
   - Prints out information about the orders being placed.

**Notes:**

- **Global Variables**:
  - `NumberOfStocksToSelectLowestOpenPrice`: Number of stocks to select with the lowest open prices.
  - `NumberOfStocksToSelectHighestOpenPrice`: Number of stocks to select with the highest open prices.
  - `CapitalRiskedPerLongTrade`: Amount of capital to risk per long trade.
  - `CapitalRiskedPerShortTrade`: Amount of capital to risk per short trade.

- **Dependencies**:
  - `Server_Order_Place`: Module that contains the `order` function used to execute orders.
  - `Directories`: Module that provides directory paths.

- **Sample Data**:
  - A sample DataFrame is provided in the `__main__` block for testing purposes.

**Usage:**

- Import or run the script as part of a trading system.
- Ensure that the `order` function and necessary directories are properly set up.
- Adjust the global variables and parameters as needed for your trading strategy.

"""
import math
from kiteconnect import KiteConnect
from Server_Order_Place import order
import logging
import pandas as pd
import os
from Directories import *
from Fetch_Positions_Data import *
import concurrent.futures
import logging
from multiprocessing import Manager

NumberOfStocksToSelectLowestOpenPrice = 5
NumberOfStocksToSelectHighestOpenPrice = 10

CapitalRiskedPerLongTrade = 84561
CapitalRiskedPerShortTrade = 120588

TargetVolatilityPerLongTrade = 0
TargetVolatilityPerShortTrade = 6398

DurationForSleep = 10
#Factor by which the limit price has to be rounded up/down resp
RoundingFactor = 0.1

ListOfOrderId = []

import multiprocessing
import logging
import traceback


def calculateQuantityKite(price, risk_per_trade):
    """
    Calculates the quantity of shares to purchase based on risk per trade.

    Parameters:
    - price (float): The price/stddev of the stock.
    - risk_per_trade (int): The maximum amount to invest per trade.

    Returns:
    - int: Number of shares to purchase.
    """
    if price <= 0:
        logging.warning(f" price {price} is not positive.")
        return 0
    
    quantity = risk_per_trade // price
    if quantity < 1:
        quantity = 1
    
    round(quantity)
    logging.info(f"Calculated quantity: {quantity} for open price/stddev: {price}")
    return quantity

def fetchLtpInstrumentKiteApi(symbol):
    nse_instrument = "NSE:" + str(symbol).upper()
    # Fetch LTP for the symbol
    ltp_data = kite.ltp([nse_instrument])
    # ltp_data is a dictionary keyed by instrument token, for example: {"NSE:INFY": {"instrument_token": 408065, "last_price": 1488.5, ...}}

    if nse_instrument in ltp_data:
        last_price = ltp_data[nse_instrument]["last_price"]
    else:
        last_price = 0
        print(f"No LTP data found for {nse_instrument}")
    
    return last_price

def prepareLongOrderKite(symbol, open_price, quantity):
    """
    Prepares the order details dictionary.

    Parameters:
    - symbol (str): The trading symbol.
    - open_price (float): The open price of the stock.
    - quantity (int): Number of shares to purchase.

    Returns:
    - dict: Order details.
    """
    
    ltp = fetchLtpInstrumentKiteApi(symbol)

    if ltp == 0:
        ltp = open_price

    longprice = ltp + (ltp * RoundingFactor)/100
    rounded_longprice = math.floor(longprice * 20) / 20

    orderDetailKite = {
        'Tradetype': 'BUY',
        'Exchange': 'NSE',
        'Tradingsymbol': str(symbol),
        'Quantity': str(quantity),
        'Variety': 'REGULAR',
        'Ordertype': 'LIMIT',
        'Product': 'MIS',  # Changed from 'CNC' to 'MIS' as per your latest code
        'Validity': 'DAY',
        'Price': str(rounded_longprice),
        'Symboltoken': '',  # Populate as needed
        'Squareoff': '',
        'Stoploss': '',
        'Broker': '',
        'Netposition': '',
        'OptionExpiryDay': '',
        'OptionContractStrikeFromATMPercent': '',
        'Trigger': '',
        'StopLossTriggerPercent': '',
        'StopLossOrderPlacePercent': '',
        'CallStrikeRequired': '',
        'PutStrikeRequired': '',
        'Hedge': '',
        'TradeFailExitRequired':'False'
    }
    logging.info(f"Prepared order for {symbol}: Quantity={quantity}, Price={open_price}")
    return orderDetailKite

def prepareShortOrderKite(symbol, open_price, quantity):
    """
    Prepares the order details dictionary.

    Parameters:
    - symbol (str): The trading symbol.
    - open_price (float): The open price of the stock.
    - quantity (int): Number of shares to purchase.

    Returns:
    - dict: Order details.
    """
    
    ltp = fetchLtpInstrumentKiteApi(symbol)
    
    if ltp == 0:
        ltp = open_price

    shortprice = ltp - (ltp * RoundingFactor)/100
    rounded_shortprice = math.floor(shortprice * 20) / 20

    orderDetailKite = {
        'Tradetype': 'SELL',
        'Exchange': 'NSE',
        'Tradingsymbol': str(symbol),
        'Quantity': str(quantity),
        'Variety': 'REGULAR',
        'Ordertype': 'LIMIT', #'MARKET',
        'Product': 'MIS',  # Changed from 'CNC' to 'MIS' as per your latest code
        'Validity': 'DAY',
        'Price': str(rounded_shortprice),#'0',
        'Symboltoken': '',  # Populate as needed
        'Squareoff': '',
        'Stoploss': '',
        'Broker': '',
        'Netposition': '',
        'OptionExpiryDay': '',
        'OptionContractStrikeFromATMPercent': '',
        'Trigger': '',
        'StopLossTriggerPercent': '',
        'StopLossOrderPlacePercent': '',
        'CallStrikeRequired': '',
        'PutStrikeRequired': '',
        'Hedge': '',
        'TradeFailExitRequired':'False'
    }
    logging.info(f"Prepared order for {symbol}: Quantity={quantity}, Price={open_price}")
    return orderDetailKite


def executeOrderKite(orderDetailKite):
    """
    Executes the order using the order function.

    Parameters:
    - orderDetailKite (dict): The order details.

    Returns:
    - None
    """
    try:
        OrderId = order(orderDetailKite)
        logging.info(f"Order placed successfully for {orderDetailKite['Tradingsymbol']}.")
        print(f"Order placed for {orderDetailKite['Tradingsymbol']}: Quantity={orderDetailKite['Quantity']}, Price={orderDetailKite['Price']}, OrderId={OrderId}, Time={datetime.now()}")
        return OrderId
    except Exception as e:
        logging.error(f"Failed to place order for {orderDetailKite['Tradingsymbol']}: {e}")
        print(f"Error placing order for {orderDetailKite['Tradingsymbol']}: {e}")
        return 0

def getTopNStocks(OrderDetails, n=NumberOfStocksToSelectLowestOpenPrice):
    """
    Sorts the DataFrame by 'Open Price' in ascending order and selects the top n stocks.

    Parameters:
    - OrderDetails (pandas DataFrame): DataFrame containing stock details.
    - n (int): Number of top stocks to select.

    Returns:
    - pandas DataFrame: DataFrame containing top n stocks.
    """
    
    LowestOpenPriceStocks = OrderDetails.head(n)
    logging.info(f"Selected top {n} stocks based on the Lowest Open Price.")
    return LowestOpenPriceStocks

def PlaceSingleOrderKite(row, trade_type, target_volatility, OrderId):
    """Helper function to place a single order."""
    symbol = row['Symbol']
    open_price = round(row['Open Price'])
    stddev = row['Std Dev']
    
    logging.info(f"Processing symbol: {symbol}, Open Price: {open_price}, stddev: {stddev}")
    print(f"Processing symbol for Kite: {symbol}, Open Price: {open_price}, stddev: {stddev}")

    quantity = int(calculateQuantityKite(stddev, target_volatility))
    if quantity == 0:
        warning_msg = f"Open price for {symbol} is zero or negative. Skipping order."
        print(f"Warning: {warning_msg}")
        logging.warning(warning_msg)
        return

    # Prepare order based on trade type
    if trade_type == 'BUY':
        orderDetailKite = prepareLongOrderKite(symbol, open_price, quantity)
    elif trade_type == 'SELL':
        orderDetailKite = prepareShortOrderKite(symbol, open_price, quantity)
    else:
        warning_msg = f"Invalid trade_type: {trade_type}. Skipping order for {symbol}."
        print(f"Warning: {warning_msg}")
        logging.warning(warning_msg)
        return

    OrderId = executeOrderKite(orderDetailKite)

    return OrderId

# Fetch input values from the file
with open(KiteEkanshLogin,'r') as a:
    content = a.readlines()

user_id= content[0].strip('\n')
user_pwd = content[1].strip('\n')
api_key = content[2].strip('\n')
api_secret = content[3].strip('\n')
totp_key= content[4].strip('\n')

kite = KiteConnect(api_key=api_key)

with open(KiteEkanshLoginAccessToken,'r') as f:
    access_tok = f.read()

kite.set_access_token(access_tok)


if __name__ == '__main__':
    # Sample DataFrame for testing
    data = {
        'Symbol': ['IDEA', 'BBB', 'CCC'],
        'Open Price': [8.1099999656677246, 200, 300],
        'Close Price': [110, 210, 310],
        'Open_PrevLow_Diff_Percent': [None, None, None]  # Add this column if needed
    }
    df = pd.DataFrame(data)
    
    # Define the output directory (ensure this path exists)
    output_directory = IntraDayDirectory
    