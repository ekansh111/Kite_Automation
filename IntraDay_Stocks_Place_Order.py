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

NumberOfStocksToSelectLowestOpenPrice = 5
NumberOfStocksToSelectHighestOpenPrice = 10

CapitalRiskedPerLongTrade = 84561
CapitalRiskedPerShortTrade = 120588

DurationForSleep = 12
#Factor by which the limit price has to be rounded up/down resp
RoundingFactor = 0.1

ListOfOrderId = []

import multiprocessing
import logging
import traceback

# Define the target function at the top level
def order_execution_target(queue, order_detail, queueOrderId):
    try:
        OrderId = execute_order(order_detail)
        queue.put(None)  # Indicate successful execution

        queueOrderId.put(OrderId)
        return order_detail
    except Exception as e:
        # Pass exception to the parent process
        queue.put(e)

def execute_order_with_timeout(order_detail, timeout=10):
    """
    Executes the order using the execute_order function with a timeout.
    
    Parameters:
    - order_detail (dict): The order details.
    - timeout (int): The maximum time (in seconds) to wait for the order execution.
    
    Returns:
    - None
    """
    # Create a queue to communicate with the subprocess
    queue = multiprocessing.Queue()

    # Create a queue to communicate with the subprocess for the OrderId
    queueOrderId = multiprocessing.Queue()

    # Start the subprocess
    process = multiprocessing.Process(target=order_execution_target, args=(queue, order_detail, queueOrderId))
    process.start()
    # Wait for the specified timeout
    process.join(timeout)

    if process.is_alive():
        # Terminate the process if it's still running
        process.terminate()
        process.join()
        logging.error(f"execute_order timed out for {order_detail['Tradingsymbol']}")
        print(f"Error: execute_order timed out for {order_detail['Tradingsymbol']}")
    else:
        #Fetch the OrderId from the subprocess
        if not queueOrderId.empty():
            OrderId = queueOrderId.get()
            ListOfOrderId.append(OrderId)
            print('List of order id')
            print(ListOfOrderId)

        # Check for exceptions raised in the subprocess
        if not queue.empty():
            exception = queue.get()
            if exception is not None:
                logging.error(f"Exception in execute_order for {order_detail['Tradingsymbol']}: {exception}")
                print(f"Error placing order for {order_detail['Tradingsymbol']}: {exception}")
                # Optionally, you can log the traceback
                traceback_str = ''.join(traceback.format_exception(None, exception, exception.__traceback__))
                logging.error(f"Traceback for {order_detail['Tradingsymbol']}:\n{traceback_str}")
                print(f"Traceback:\n{traceback_str}")
        else:
            # Order executed successfully
            logging.info(f"Order placed successfully for {order_detail['Tradingsymbol']}.")
            print(f"Order placed for {order_detail['Tradingsymbol']}: Quantity={order_detail['Quantity']}, Price={order_detail['Price']}")


def configure_logging(log_file="intraday_orders.log"):
    """
    Configures the logging settings.

    Parameters:
    - log_file (str): The filename for the log file.

    Returns:
    - None
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    logging.info("Logging is configured.")

def validate_order_details(OrderDetails):
    """
    Validates the input OrderDetails DataFrame.

    Parameters:
    - OrderDetails (pandas DataFrame): DataFrame containing stock details.

    Returns:
    - bool: True if valid, False otherwise.
    """
    required_columns = ['Symbol', 'Open Price']
    if not isinstance(OrderDetails, pd.DataFrame):
        logging.error("OrderDetails is not a pandas DataFrame.")
        print("Error: OrderDetails is not a pandas DataFrame.")
        return False
    
    missing_columns = [col for col in required_columns if col not in OrderDetails.columns]
    if missing_columns:
        logging.error(f"Missing columns in OrderDetails: {missing_columns}")
        print(f"Error: Missing columns in OrderDetails: {missing_columns}")
        return False
    
    logging.info("OrderDetails validation passed.")
    return True

def get_top_n_stocks(OrderDetails, n=NumberOfStocksToSelectLowestOpenPrice):
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

def get_bottom_n_stocks(OrderDetails, n=NumberOfStocksToSelectHighestOpenPrice):
    """
    Sorts the DataFrame by 'Open Price' in ascending order and selects the bottom n stocks.

    Parameters:
    - OrderDetails (pandas DataFrame): DataFrame containing stock details.
    - n (int): Number of top stocks to select.

    Returns:
    - pandas DataFrame: DataFrame containing bottom n stocks.
    """
    HighestOpenPriceStocks = OrderDetails.tail(n)
    logging.info(f"Selected bottom {n} stocks based on Highest Open Price.")
    return HighestOpenPriceStocks


def display_selected_stocks(LowestOpenPriceStocks, NumberOfStocks):
    """
    Displays and logs the selected stocks' details.

    Parameters:
    - LowestOpenPriceStocks (pandas DataFrame): DataFrame of selected stocks.

    Returns:
    - None
    """
    print('Top' + str(NumberOfStocks) + 'rows based on Open Price:')
    print(LowestOpenPriceStocks)
    logging.info(f"Top {NumberOfStocks} stocks:\n{LowestOpenPriceStocks}")
    
    if 'Open_PrevLow_Diff_Percent' in LowestOpenPriceStocks.columns:
        print("\nSymbols, Open Prices, and Open_PrevLow_Diff_Percent:")
        print(LowestOpenPriceStocks[['Symbol', 'Open Price', 'Open_PrevLow_Diff_Percent']])
        logging.info("Displayed Symbols, Open Prices, and Open_PrevLow_Diff_Percent.")
    else:
        print("\nSymbols and Open Prices:")
        print(LowestOpenPriceStocks[['Symbol', 'Open Price']])
        logging.info("Displayed Symbols and Open Prices.")

def calculate_quantity(open_price, risk_per_trade=10000):
    """
    Calculates the quantity of shares to purchase based on risk per trade.

    Parameters:
    - open_price (float): The open price of the stock.
    - risk_per_trade (int): The maximum amount to invest per trade.

    Returns:
    - int: Number of shares to purchase.
    """
    if open_price <= 0:
        logging.warning(f"Open price {open_price} is not positive.")
        return 0
    
    quantity = risk_per_trade // open_price
    if quantity < 1:
        quantity = 1
    logging.info(f"Calculated quantity: {quantity} for open price: {open_price}")
    return quantity

def fetch_ltp_instrument(symbol):
    nse_instrument = "NSE:" + str(symbol).upper()
    # Fetch LTP for the symbol
    ltp_data = kite.ltp([nse_instrument])
    # ltp_data is a dictionary keyed by instrument token, for example: {"NSE:INFY": {"instrument_token": 408065, "last_price": 1488.5, ...}}

    if nse_instrument in ltp_data:
        last_price = ltp_data[nse_instrument]["last_price"]
        print(f"LTP for {nse_instrument} is {last_price}")
    else:
        print(f"No LTP data found for {nse_instrument}")
    
    return last_price

def prepare_long_order(symbol, open_price, quantity):
    """
    Prepares the order details dictionary.

    Parameters:
    - symbol (str): The trading symbol.
    - open_price (float): The open price of the stock.
    - quantity (int): Number of shares to purchase.

    Returns:
    - dict: Order details.
    """
    
    ltp = fetch_ltp_instrument(symbol)

    longprice = ltp + (ltp * RoundingFactor)/100
    rounded_longprice = math.floor(longprice * 20) / 20

    order_detail = {
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
        'Hedge': ''
    }
    logging.info(f"Prepared order for {symbol}: Quantity={quantity}, Price={open_price}")
    return order_detail

def prepare_short_order(symbol, open_price, quantity):
    """
    Prepares the order details dictionary.

    Parameters:
    - symbol (str): The trading symbol.
    - open_price (float): The open price of the stock.
    - quantity (int): Number of shares to purchase.

    Returns:
    - dict: Order details.
    """
    
    ltp = fetch_ltp_instrument(symbol)

    shortprice = ltp - (ltp * RoundingFactor)/100
    rounded_shortprice = math.floor(shortprice * 20) / 20

    order_detail = {
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
        'Hedge': ''
    }
    logging.info(f"Prepared order for {symbol}: Quantity={quantity}, Price={open_price}")
    return order_detail


def execute_order(order_detail):
    """
    Executes the order using the order function.

    Parameters:
    - order_detail (dict): The order details.

    Returns:
    - None
    """
    try:
        OrderId = order(order_detail)
        logging.info(f"Order placed successfully for {order_detail['Tradingsymbol']}.")
        print(f"Order placed for {order_detail['Tradingsymbol']}: Quantity={order_detail['Quantity']}, Price={order_detail['Price']}, OrderId={OrderId}")
        return OrderId
    except Exception as e:
        logging.error(f"Failed to place order for {order_detail['Tradingsymbol']}: {e}")
        print(f"Error placing order for {order_detail['Tradingsymbol']}: {e}")


def PlaceIntradayOrders(OrderDetailsLong, OrderDetailsShort, trade_type1, trade_type_2, risk_per_trade_long= CapitalRiskedPerLongTrade, risk_per_trade_short= CapitalRiskedPerShortTrade):
    """
    Orchestrates the placement of intraday buy/sell orders based on Open Price and risk per trade.

    Parameters:
    - OrderDetails (pandas DataFrame): DataFrame containing stock details.
    - trade_type (str): Type of trade, either 'BUY' or 'SELL'.
    - risk_per_trade (int, optional): Maximum amount to invest per trade. Defaults to CapitalRiskedPerTrade.

    Returns:
    - None
    """
    configure_logging()
    logging.info("Starting PlaceIntradayOrders function.")
    
    if not validate_order_details(OrderDetailsLong):
        logging.error("OrderDetails validation failed. Exiting function.")
        return
    
    if not validate_order_details(OrderDetailsShort):
        logging.error("OrderDetails validation failed. Exiting function.")
        return
    # Select stocks with lowest open prices
    LowestOpenPriceStocks = get_top_n_stocks(OrderDetailsLong, n=NumberOfStocksToSelectLowestOpenPrice)
    # Select stocks with highest open prices
    HighestOpenPriceStocks = get_top_n_stocks(OrderDetailsShort, n=NumberOfStocksToSelectHighestOpenPrice)  # **[Change] Selecting Highest Open Price Stocks**

    # Display selected stocks for both lowest and highest open prices
    display_selected_stocks(LowestOpenPriceStocks, NumberOfStocksToSelectLowestOpenPrice)
    display_selected_stocks(HighestOpenPriceStocks, NumberOfStocksToSelectHighestOpenPrice)  # **[Change] Displaying Highest Open Price Stocks**
    
    # Check if both sets are empty
    if LowestOpenPriceStocks.empty and HighestOpenPriceStocks.empty:
        logging.warning("No stocks available to place orders after selecting top N from both sets.")
        print("No stocks available to place orders.")
        return
    
    # Process LowestOpenPriceStocks
    if not LowestOpenPriceStocks.empty:
        print("Entering the order placement loop for Lowest Open Price Stocks.")
        logging.info("Entering the order placement loop for Lowest Open Price Stocks.")
        
        for index, row in LowestOpenPriceStocks.iterrows():
            symbol = row['Symbol']
            open_price = row['Open Price']
            open_price = round(open_price)
            
            logging.info(f"Processing symbol: {symbol}, Open Price: {open_price}")
            print(f"Processing symbol: {symbol}, Open Price: {open_price}")
            
            quantity = calculate_quantity(open_price, risk_per_trade_long)
            if quantity == 0:
                warning_msg = f"Open price for {symbol} is zero or negative. Skipping order."
                print(f"Warning: {warning_msg}")
                logging.warning(warning_msg)
                continue
            
            if trade_type1 == 'BUY':
                order_detail = prepare_long_order(symbol, open_price, quantity)
            elif trade_type1 == 'SELL':
                order_detail = prepare_short_order(symbol, open_price, quantity)
            else:
                warning_msg = f"Invalid trade_type: {trade_type1}. Skipping order for {symbol}."
                print(f"Warning: {warning_msg}")
                logging.warning(warning_msg)
                continue
            
            print(order_detail)
            execute_order_with_timeout(order_detail, timeout=10)
    
    if not HighestOpenPriceStocks.empty:
        print("Entering the order placement loop for Highest Open Price Stocks.")
        logging.info("Entering the order placement loop for Highest Open Price Stocks.")
        
        for index, row in HighestOpenPriceStocks.iterrows():
            symbol = row['Symbol']
            open_price = row['Open Price']
            open_price = round(open_price)
            
            logging.info(f"Processing symbol: {symbol}, Open Price: {open_price}")
            print(f"Processing symbol: {symbol}, Open Price: {open_price}")
            
            quantity = int(calculate_quantity(open_price, risk_per_trade_short))
            if quantity == 0:
                warning_msg = f"Open price for {symbol} is zero or negative. Skipping order."
                print(f"Warning: {warning_msg}")
                logging.warning(warning_msg)
                continue
            
            if trade_type_2 == 'BUY':
                order_detail = prepare_long_order(symbol, open_price, quantity)
            elif trade_type_2 == 'SELL':
                order_detail = prepare_short_order(symbol, open_price, quantity)
            else:
                warning_msg = f"Invalid trade_type: {trade_type_2}. Skipping order for {symbol}."
                print(f"Warning: {warning_msg}")
                logging.warning(warning_msg)
                continue
            
            print(order_detail)
            execute_order_with_timeout(order_detail, timeout=10)
    # **[End of New Section]**
    print('consolidated orderid are:')
    print(ListOfOrderId)

    #Get the order status and wait for the desired time, if order is still not placed, then convert to market
    time.sleep(DurationForSleep)
    OrderType = 'MARKET'
    get_order_status(kite, ListOfOrderId, OrderType, ReorderFlag=1)
    
    logging.info("Completed PlaceIntradayOrders function.")
    print("Finished placing intraday orders.")

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
    
    # Place intraday orders
    PlaceIntradayOrders(df, 'BUY','SELL')
