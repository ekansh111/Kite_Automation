"""
This script automates the placement of intraday orders based on stock data,
routing orders through Angel SmartAPI instead of Kite API.

Main Functionalities:
1. Order Placement Automation:
   - Reads stock data from a pandas DataFrame.
   - Selects stocks based on criteria such as lowest and highest open prices.
   - Calculates the quantity to trade based on the open price and capital risked per trade.
   - Prepares order details for both long (buy) and short (sell) positions.
   - Executes orders using the SmartAPI connection’s placeOrder method.

2. Configuration and Validation:
   - Configures logging to capture important events and errors.
   - Validates the input DataFrame to ensure it contains required columns.

3. Stock Selection:
   - Selects the top N stocks with the lowest open prices.
   - Selects the bottom N stocks with the highest open prices.

4. Order Preparation:
   - Prepares order details for long (BUY) and short (SELL) trades.
   - Calculates the quantity of shares to trade based on risk per trade and open price.

5. Order Execution:
   - Executes the prepared orders concurrently using a thread pool.

Notes:
- Global variables such as numberOfStocksToSelectLowestOpenPrice, capitalRiskedPerLongTrade, etc.
  can be adjusted as needed.
- Dependencies include Directories, Server_Order_Handler (providing establishConnectionAngelApi),
  and any helper modules for order status.
"""

import math
from SmartApi import SmartConnect
import logging
import pandas as pd
import os
from Directories import *
import concurrent.futures
from multiprocessing import Manager
from Server_Order_Handler import EstablishConnectionAngelAPI
import yfinance as yf
from AngelInstrumentTokenHandle import FetchAngelInstrumentSymbolToken
from IntraDay_Stocks_Place_Order import *  # Additional functions if needed
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Global configuration variables
numberOfStocksToSelectLowestOpenPrice = 5
numberOfStocksToSelectHighestOpenPrice = 10

capitalRiskedPerLongTrade = 84561
capitalRiskedPerShortTrade = 120588

targetVolatilityPerLongTrade = 0
targetVolatilityPerShortTrade = 6398

durationForSleep = 10
RoundingFactor = 0.1

# -------------------------------------------------------------------
# Logging and validation functions
# -------------------------------------------------------------------

def configureLoggingAngel(logFile="intraday_orders.log"):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(logFile),
            logging.StreamHandler()
        ]
    )
    logging.info("Logging is configured.")


# -------------------------------------------------------------------
# Order preparation functions (converted for SmartAPI)
# -------------------------------------------------------------------

def calculateQuantityAngel(price, riskPerTrade):
    if price <= 0:
        logging.warning(f"Price {price} is not positive.")
        return 0
    quantity = riskPerTrade // price
    if quantity < 1:
        quantity = 1
    logging.info(f"Calculated quantity: {quantity} for price/stddev: {price}")
    return quantity


def removeEqFromSymbolName(symbol: str) -> str:
    """
    Removes the trailing '-EQ' from a symbol name if it exists.
    """
    return symbol[:-3] if symbol.endswith("-EQ") else symbol

def roundClosingPrice(raw_price: float) -> float:
    """
    Round *down* an input price to the exchange-allowed tick size
    
    Parameters
    ----------
    raw_price : float
        The un-rounded price.

    Returns
    -------
    float
        Price rounded **down** to the nearest valid tick.
    """

    if raw_price < 250:
        multiplier = 100      # 1 / 0.01

    elif raw_price < 1000:
        multiplier = 20       # 1 / 0.05
    elif raw_price < 5000:
        multiplier = 10       # 1 / 0.10
    elif raw_price < 10000:
        multiplier = 2        # 1 / 0.50
    elif raw_price < 20000:
        multiplier = 1        # 1 / 1.00

    else:
        multiplier = 0.2      # 1 / 5.00  (price > 20000)

    return math.floor(raw_price * multiplier) / multiplier
    


def prepareLongOrderAngel(symbol, openPrice, quantity, symbolToken):
    """
    Prepares the order details for a BUY order using Angel SmartAPI.
    """
    
    strippedSymbol = removeEqFromSymbolName(symbol)
    ltp = fetchLtpInstrumentKiteApi(strippedSymbol)  # Assumes fetchLtpInstrument is defined elsewhere
    
    if ltp == 0:
        ltp = openPrice
    
    longPrice = ltp + (ltp * RoundingFactor) / 100
    longPriceFloat = float(longPrice.iloc[0]) if hasattr(longPrice, 'iloc') else float(longPrice)
    roundedLongPrice = roundClosingPrice(longPriceFloat)

    orderDetail = {
        "variety": "NORMAL",
        "tradingsymbol": str(symbol),
        "symboltoken": symbolToken,
        "transactiontype": "BUY",
        "exchange": "NSE",
        "ordertype": "LIMIT",
        "producttype": "INTRADAY",
        "duration": "DAY",
        "price": str(roundedLongPrice),
        "squareoff": "0",
        "stoploss": "0",
        "quantity": str(quantity),  # or str(quantity) if quantity is more than 1
        "triggerprice": "0",
        "TradeFailExitRequired":"False"
    }
    logging.info(f"Prepared long order for {symbol}: Quantity={quantity}, Base Price={openPrice}")
    return orderDetail


def prepareShortOrderAngel(symbol, openPrice, quantity, symbolToken):
    """
    Prepares the order details for a SELL order using Angel SmartAPI.
    """

    strippedSymbol = removeEqFromSymbolName(symbol)
    ltp = fetchLtpInstrumentKiteApi(strippedSymbol)
    
    if ltp == 0:
        ltp = openPrice
    
    shortPrice = ltp - (ltp * RoundingFactor) / 100
    shortPriceFloat = float(shortPrice.iloc[0]) if hasattr(shortPrice, 'iloc') else float(shortPrice)
    roundedShortPrice = roundClosingPrice(shortPriceFloat)
    
    orderDetail = {
        "variety": "NORMAL",
        "tradingsymbol": str(symbol),
        "symboltoken": symbolToken,
        "transactiontype": "SELL",
        "exchange": "NSE",
        "ordertype": "LIMIT",
        "producttype": "INTRADAY",
        "duration": "DAY",
        "price": str(roundedShortPrice),
        "squareoff": "0",
        "stoploss": "0",
        "quantity": str(quantity),
        "triggerprice": "0",
        "TradeFailExitRequired":"False"
    }
    logging.info(f"Prepared short order for {symbol}: Quantity={quantity}, Base Price={openPrice}")
    return orderDetail

# -------------------------------------------------------------------
# Order execution via SmartAPI
# -------------------------------------------------------------------

def executeOrder(smartApi, orderDetail):
    """
    Executes the order using SmartAPI's placeOrder method.
    """
    try:
        orderId = smartApi.placeOrder(orderDetail)
        logging.info(f"Order placed successfully for {orderDetail['tradingsymbol']}.")
        print(f"Order placed for {orderDetail['tradingsymbol']}: Quantity={orderDetail['quantity']}, "
              f"Price={orderDetail['price']}, OrderId={orderId}, Time={datetime.now()}")
        return orderId
    except Exception as e:
        logging.error(f"Failed to place order for {orderDetail['tradingsymbol']}: {e}")
        print(f"Error placing order for {orderDetail['tradingsymbol']}: {e}")
        return None

# -------------------------------------------------------------------
# Parallel order placement functions
# -------------------------------------------------------------------

def placeIntradayAngelOrders(lowestOpenPriceStocks, highestOpenPriceStocks, tradeType1, tradeType2, smartApi, OrderTriggerTime):
    """
    Processes orders for intraday trading (example: SELL orders) concurrently.
    """
    configureLoggingAngel()
    logging.info("Starting placeIntradayAngelOrders function.")
    
    # Process highest open price stocks concurrently
    orderIds = processSelectedStocks(highestOpenPriceStocks, tradeType2, TargetVolatilityPerShortTrade,
                                     "Highest Open Price Stocks", smartApi, OrderTriggerTime)
    
    print("Consolidated order IDs are:", orderIds)
    logging.info("Completed placeIntradayAngelOrders function.")


def processSelectedStocks(selectedStocks, tradeType, targetVolatility, description, smartApi, OrderTriggerTime):
    """
    Processes the selected stocks concurrently.
    
    Parameters:
        selectedStocks (DataFrame): DataFrame of selected stocks.
        tradeType (str): e.g., "SELL".
        targetVolatility (int): Volatility parameter for quantity calculation.
        description (str): Description of the selection.
        smartApi (SmartConnect): The connected SmartAPI instance.
    
    Returns:
        list: A list of order IDs.
    """
    logging.info(f"Entering the order placement loop for {description}.")
    orderIds = []
    while True:
        if 1==1:
            with ThreadPoolExecutor(max_workers=10) as executor:
                futureToRow = {
                    executor.submit(placeSingleOrderAngel, row, tradeType, targetVolatility, smartApi, OrderTriggerTime): row
                    for _, row in selectedStocks.iterrows()
                }
                for future in as_completed(futureToRow):
                    try:
                        result = future.result(timeout=10)
                        if result is not None:
                            orderIds.append(result)
                    except TimeoutError:
                        print("timeouterror")
                        logging.error("Order processing timed out")
                    except Exception as e:
                        print('Exception caught', e)
                        logging.error(f"Error processing row: {e}")
            executor.shutdown(wait=True)  # Ensure threads exit
            print(f"Completed processing stocks for {description}.")
            return orderIds

        else:
            time.sleep(0.01)


def placeSingleOrderAngel(row, tradeType, targetVolatility, smartApi, OrderTriggerTime):
    symbol = row['Symbol'] + "-EQ"
    openPrice = round(row['Open Price'])
    stdDev = row['Std Dev']
    orderSymbol = {"Tradingsymbol": symbol}

    symbolToken = FetchAngelInstrumentSymbolToken(orderSymbol)
    # Always define orderId so the function has a valid variable to return
    orderId = None
    while True:
        if OrderTriggerTime == datetime.now().strftime("%H:%M:%S"):

            logging.info(f"Processing symbol: {symbol}, Open Price: {openPrice}, Std Dev: {stdDev}")
            print(f"Processing symbol: {symbol}, Open Price: {openPrice}, Std Dev: {stdDev}")
            quantity = int(calculateQuantityAngel(stdDev, targetVolatility))
            if quantity == 0:
                warningMsg = f"Open price for {symbol} is zero or negative. Skipping order."
                print(f"Warning: {warningMsg}")
                logging.warning(warningMsg)
                return None
            
            # Prepare the order
            if tradeType.upper() == "BUY":
                orderDetail = prepareLongOrderAngel(symbol, openPrice, quantity, symbolToken)
            elif tradeType.upper() == "SELL":
                orderDetail = prepareShortOrderAngel(symbol, openPrice, quantity, symbolToken)
            else:
                warningMsg = f"Invalid tradeType: {tradeType}. Skipping order for {symbol}."
                print(f"Warning: {warningMsg}")
                logging.warning(warningMsg)
                return None
            
            orderId = executeOrder(smartApi, orderDetail)

            if orderId is None:
                # fallback to placing order via Kite
                orderId = PlaceSingleOrderKite(row, tradeType, targetVolatility, "")
            # If not 17:22:00, orderId will remain `None`
            return orderId
        else:
            time.sleep(0.01)



# -------------------------------------------------------------------
# Main execution block
# -------------------------------------------------------------------

if __name__ == '__main__':
    # Sample DataFrame for testing
    sampleData = {
        'Symbol': ['IDEA', 'BBB', 'CCC'],
        'Open Price': [8.1099999656677246, 200, 300],
        'Close Price': [110, 210, 310],
        'Std Dev': [1, 2, 3],
        'Open_PrevLow_Diff_Percent': [None, None, None]
    }
    df = pd.DataFrame(sampleData)
    
    # Ensure the output directory exists (from Directories module)
    outputDirectory = IntraDayDirectory  

    # Execute intraday orders (for this example, using SELL orders)
    # Adjust tradeType parameters as needed (here we use "BUY" for long orders and "SELL" for short orders)
    placeIntradayAngelOrders(df, df, "BUY", "SELL", EstablishConnectionAngelAPI({"User": "E51339915"})[0])
