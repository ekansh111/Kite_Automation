from Server_Order_Place import order
import logging
import pandas as pd
import os
from Directories import *

NumberOfStocksToSelectLowestOpenPrice = 5
NumberOfStocksToSelectHighestOpenPrice = 10

CapitalRiskedPerLongTrade = 84561
CapitalRiskedPerShortTrade = 120588

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
    order_detail = {
        'Tradetype': 'BUY',
        'Exchange': 'NSE',
        'Tradingsymbol': str(symbol),
        'Quantity': str(quantity),
        'Variety': 'REGULAR',
        'Ordertype': 'MARKET',#'LIMIT',
        'Product': 'MIS',  # Changed from 'CNC' to 'MIS' as per your latest code
        'Validity': 'DAY',
        'Price': '0',#str(open_price),
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
    order_detail = {
        'Tradetype': 'SELL',
        'Exchange': 'NSE',
        'Tradingsymbol': str(symbol),
        'Quantity': str(quantity),
        'Variety': 'REGULAR',
        'Ordertype': 'MARKET',#'LIMIT', #'MARKET',
        'Product': 'MIS',  # Changed from 'CNC' to 'MIS' as per your latest code
        'Validity': 'DAY',
        'Price': '0',#str(open_price),#'0',
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
        order(order_detail)
        logging.info(f"Order placed successfully for {order_detail['Tradingsymbol']}.")
        print(f"Order placed for {order_detail['Tradingsymbol']}: Quantity={order_detail['Quantity']}, Price={order_detail['Price']}")
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
            execute_order(order_detail)
    
    # **[New Section] Process HighestOpenPriceStocks**
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
            execute_order(order_detail)
    # **[End of New Section]**
    
    logging.info("Completed PlaceIntradayOrders function.")
    print("Finished placing intraday orders.")

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
