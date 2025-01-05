"""
This script is designed to fetch historical stock data for a list of symbols from the Nifty 500 constituents,
compute technical indicators, filter stocks based on specific criteria, and optionally place intraday orders.

**Main Functionalities:**

1. **Read Stock Symbols**:
   - Reads a CSV file containing stock symbols (`Nifty500ConstituentList`).

2. **Fetch Historical Data**:
   - Uses the `yfinance` library to download historical **Open**, **Low**, **High**, and **Close** prices for the specified symbols over a defined date range.
   - Fetches data in batches to avoid overwhelming the API.

3. **Compute Technical Indicators**:
   - **Simple Moving Average (SMA)**: Calculates the SMA over a specified window (default is 20 days).
   - **Standard Deviation (Std Dev)**: Calculates the standard deviation over a specified window (default is 90 days).
   - **Open_PrevLow_Diff**: Computes the difference between today's Open price and yesterday's Low price.
   - **Open_PrevLow_Diff_Percent**: Calculates the percentage difference between today's Open and yesterday's Low.
   - **Open_PrevHigh_Diff**: Computes the difference between today's Open price and yesterday's High price.
   - **Open_PrevHigh_Diff_Percent**: Calculates the percentage difference between today's Open and yesterday's High.
   - **Open_Today_Close_Diff**: Calculates the percentage return from today's Open to today's Close.

4. **Data Saving**:
   - Saves the fetched data and computed indicators to CSV files.
   - Saves sorted data based on `Open_PrevLow_Diff_Percent` and `Open_PrevHigh_Diff_Percent`.

5. **Filtering Stocks**:
   - Filters stocks where the **Open Price** is higher than the 20-day SMA (for potential long positions).
   - Filters stocks where the **Open Price** is lower than the 20-day SMA (for potential short positions).

6. **Place Intraday Orders** (Optional):
   - Determines trade type based on current time:
     - **Before 11 AM**: `BUY` (long positions).
     - **After 11 AM**: `SELL` (short positions).
   - Places intraday orders using the `PlaceIntradayOrders` function imported from the `IntraDay_Stocks_Place_Order` module.
   - The `PlaceOrderIK6635` flag controls whether to place orders.

7. **Logging and Error Handling**:
   - Uses the `logging` module to capture warnings and errors.
   - Handles exceptions during data fetching and processing.
   - Logs missing data and other issues.

8. **Multiprocessing**:
   - Uses multiprocessing to speed up data processing for multiple symbols.

**Notes:**

- The script is tailored for the **National Stock Exchange of India (NSE)**, appending `.NS` to the stock symbols.
- The directories and file paths (`IntraDayDirectory`, `Nifty500ConstituentList`) are imported from the `Directories` module.
- Adjust the `PlaceOrderIK6635` flag and other parameters as needed.
- Ensure all required modules are installed and necessary files are available.

**Usage:**

- Run the script to fetch data, compute indicators, and optionally place intraday orders based on the criteria.
- The script can be scheduled to run daily to automate data fetching and order placement.

**Dependencies:**

- **Python 3.x**
- **Required Libraries**: `pandas`, `yfinance`, `datetime`, `multiprocessing`, `logging`, etc.
- **Custom Modules**:
  - `IntraDay_Stocks_Place_Order` (contains the `PlaceIntradayOrders` function).
  - `Directories` (contains directory paths like `IntraDayDirectory`, `Nifty500ConstituentList`).

"""

import os
import pandas as pd
import time
import logging
import yfinance as yf
from itertools import islice
from datetime import datetime, timedelta
from IntraDay_Stocks_Place_Order import PlaceIntradayOrders
from multiprocessing import Pool, cpu_count
from Directories import *
from Push_File_To_Email import *
from Email_Config import *

# Set a smaller batch size to avoid overwhelming the API
total_batch_size = 500
# Flag to decide if to place order on Zerodha acc
PlaceOrderIK6635 = True
#Date for which script will run
selected_date_input = str(datetime.today().date())#input("Selected Date: ").strip() or '2024-10-21'
#Should data be sent by email
#SendFileDataByEmail = True
SendFileDataByEmail = True
#Time at which the order needs to be sent
OrderTriggerTime = '09:15:00'

def read_csv_file(file_path, delimiter=','):
    """
    Reads the CSV file and returns a pandas DataFrame.
    """
    try:
        df = pd.read_csv(file_path, delimiter=delimiter)
        print(f"Successfully read the CSV file: {file_path}")
        print(f"Columns Found: {df.columns.tolist()}")
        return df
    except FileNotFoundError:
        print(f"Error: The file {file_path} does not exist.")
        logging.error(f"File not found: {file_path}")
        exit(1)
    except pd.errors.ParserError as e:
        print(f"Error: Failed to parse the CSV file. {e}")
        logging.error(f"Parser error for file {file_path}: {e}")
        exit(1)

def batch_iterator(iterable, batch_size):
    """
    Yields successive batches of size batch_size from iterable.
    """
    it = iter(iterable)
    while True:
        batch = list(islice(it, batch_size))
        if not batch:
            break
        yield batch

def fetch_ltp(symbols, dates, sma_window, std_dev_window, batch_size=total_batch_size, pause=1):
    """
    Fetches the Closing Price (Close), Open Price (Open), High Price (High), and Low Price (Low) for each stock symbol
    on specified dates using yfinance, computes SMA and Std Dev, calculates the difference
    between today's Open and yesterday's Low and High, computes the percentage differences,
    and calculates the percentage return from today's Open to today's Close.

    Returns:
    - pandas DataFrame: Contains 'Symbol', 'Date', 'Open Price', 'Low Price', 'High Price', 'Close Price',
      'SMA', 'Std Dev', 'Open_PrevLow_Diff', 'Open_PrevLow_Diff_Percent',
      'Open_PrevHigh_Diff', 'Open_PrevHigh_Diff_Percent', 'Open_Today_Close_Diff'.
    """
    # Initialize a list to store the data
    close_prices = []

    # Configure logging to capture warnings and errors
    logging.basicConfig(
        filename='fetch_close_prices.log',
        filemode='a',
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.WARNING  # Set logging level to WARNING
    )

    total_batches = (len(symbols) + batch_size - 1) // batch_size

    # Convert date strings to datetime objects and sort them
    date_objs = sorted([datetime.strptime(date, "%Y-%m-%d") for date in dates])
    if not date_objs:
        logging.error("No valid dates provided.")
        exit(1)

    # Define the overall date range
    start_date = date_objs[0]
    end_date = date_objs[-1] + timedelta(days=1)  # Include the last day

    # Format dates for yfinance
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    # Create a set for faster lookup
    desired_dates = set(date.strftime("%Y-%m-%d") for date in date_objs)

    for idx, batch in enumerate(batch_iterator(symbols, batch_size), start=1):
        symbols_with_suffix = [symbol + ".NS" for symbol in batch]

        try:
            print('Beginning the download')
            # Fetch data for the current batch over the entire date range
            data = yf.download(
                tickers=symbols_with_suffix,
                start=start_str,
                end=end_str,
                interval="1d",
                group_by='ticker',
                threads=True,
                progress=False
            )
        except Exception as e:
            logging.error(f"Error fetching data for batch {idx}: {e}")
            # Assign None to all symbols in this batch and continue
            for symbol in batch:
                empty_data = pd.DataFrame({
                    'Symbol': [symbol] * len(desired_dates),
                    'Date': [date.strftime("%Y-%m-%d") for date in date_objs],
                    'Open Price': [None] * len(desired_dates),
                    'Low Price': [None] * len(desired_dates),
                    'High Price': [None] * len(desired_dates),
                    'Close Price': [None] * len(desired_dates),
                    'SMA': [None] * len(desired_dates),
                    'Std Dev': [None] * len(desired_dates),
                    'Open_PrevLow_Diff': [None] * len(desired_dates),
                    'Open_PrevLow_Diff_Percent': [None] * len(desired_dates),
                    'Open_PrevHigh_Diff': [None] * len(desired_dates),
                    'Open_PrevHigh_Diff_Percent': [None] * len(desired_dates),
                    'Open_Today_Close_Diff': [None] * len(desired_dates),
                })
                close_prices.append(empty_data)
            time.sleep(pause)
            continue

        if len(batch) == 1:
            # When only one ticker is fetched, the DataFrame does not have a multi-level column
            data.columns = pd.MultiIndex.from_product([[symbols_with_suffix[0]], data.columns])

        # Prepare arguments for multiprocessing
        args_list = [(symbol, data, desired_dates, date_objs, sma_window, std_dev_window) for symbol in batch]

        # Use multiprocessing Pool to process symbols in parallel
        with Pool(processes=min(cpu_count(), len(batch))) as pool:
            results = pool.map(process_symbol_data, args_list)
            close_prices.extend(results)

        # Pause between batches to respect rate limits
        time.sleep(pause)

    # Convert the list of DataFrames to a single DataFrame
    df_close = pd.concat(close_prices, ignore_index=True)

    # Remove duplicate entries if any
    df_close = df_close.drop_duplicates(subset=['Symbol', 'Date'])

    return df_close

def process_symbol_data(args):
    """
    Processes data for a single symbol, computes required metrics, and returns a DataFrame.
    """
    symbol, data, desired_dates, date_objs, sma_window, std_dev_window = args
    symbol_with_suffix = symbol + ".NS"
    try:
        if symbol_with_suffix in data.columns.levels[0]:
            ticker_data = data[symbol_with_suffix].copy()
        else:
            ticker_data = pd.DataFrame()

        if not ticker_data.empty:
            # Ensure the index is a DatetimeIndex
            if not isinstance(ticker_data.index, pd.DatetimeIndex):
                ticker_data.index = pd.to_datetime(ticker_data.index, errors='coerce')
                if ticker_data.index.isnull().all():
                    raise ValueError("All dates could not be converted to datetime.")

            # Sort the data by date just in case
            ticker_data = ticker_data.sort_index()
            ticker_data = ticker_data.dropna()
            # Compute SMA and Std Dev using rolling windows
            ticker_data['SMA'] = ticker_data['Close'].rolling(window=sma_window).mean()
            ticker_data['Std Dev'] = ticker_data['Close'].rolling(window=std_dev_window).std()

            # Compute Previous Low and High for calculating the differences
            ticker_data['Prev_Low'] = ticker_data['Low'].shift(1)
            ticker_data['Prev_High'] = ticker_data['High'].shift(1)

            # Compute Open_PrevLow_Diff and Open_PrevLow_Diff_Percent
            ticker_data['Open_PrevLow_Diff'] = ticker_data['Open'] - ticker_data['Prev_Low']
            ticker_data['Open_PrevLow_Diff_Percent'] = (
                ticker_data['Open_PrevLow_Diff'] / ticker_data['Prev_Low']
            ) * 100

            # Compute Open_PrevHigh_Diff and Open_PrevHigh_Diff_Percent
            ticker_data['Open_PrevHigh_Diff'] = ticker_data['Open'] - ticker_data['Prev_High']
            ticker_data['Open_PrevHigh_Diff_Percent'] = (
                ticker_data['Open_PrevHigh_Diff'] / ticker_data['Prev_High']
            ) * 100

            # Handle division by zero or NaN in Prev_Low and Prev_High
            ticker_data['Open_PrevLow_Diff_Percent'] = ticker_data['Open_PrevLow_Diff_Percent'].replace(
                [float('inf'), -float('inf')], pd.NA
            )
            ticker_data['Open_PrevHigh_Diff_Percent'] = ticker_data['Open_PrevHigh_Diff_Percent'].replace(
                [float('inf'), -float('inf')], pd.NA
            )

            # Compute Open_Today_Close_Diff
            ticker_data['Open_Today_Close_Diff'] = (
                (ticker_data['Close'] - ticker_data['Open']) / ticker_data['Open']
            ) * 100

            # Convert index to date strings
            ticker_data['Date'] = ticker_data.index.strftime("%Y-%m-%d")

            # Filter to desired dates
            ticker_data = ticker_data[ticker_data['Date'].isin(desired_dates)]

            if not ticker_data.empty:
                # Add Symbol column
                ticker_data['Symbol'] = symbol

                # Reset index
                ticker_data = ticker_data.reset_index(drop=True)

                # Select the necessary columns
                ticker_data = ticker_data[[
                    'Symbol', 'Date', 'Open', 'Low', 'High', 'Close', 'SMA', 'Std Dev',
                    'Open_PrevLow_Diff', 'Open_PrevLow_Diff_Percent',
                    'Open_PrevHigh_Diff', 'Open_PrevHigh_Diff_Percent',
                    'Open_Today_Close_Diff'
                ]]

                # Rename columns to match expected output
                ticker_data.columns = [
                    'Symbol', 'Date', 'Open Price', 'Low Price', 'High Price', 'Close Price',
                    'SMA', 'Std Dev', 'Open_PrevLow_Diff', 'Open_PrevLow_Diff_Percent',
                    'Open_PrevHigh_Diff', 'Open_PrevHigh_Diff_Percent',
                    'Open_Today_Close_Diff'
                ]

                return ticker_data
            else:
                # No data for desired dates
                empty_data = pd.DataFrame({
                    'Symbol': [symbol] * len(desired_dates),
                    'Date': [date.strftime("%Y-%m-%d") for date in date_objs],
                    'Open Price': [None] * len(desired_dates),
                    'Low Price': [None] * len(desired_dates),
                    'High Price': [None] * len(desired_dates),
                    'Close Price': [None] * len(desired_dates),
                    'SMA': [None] * len(desired_dates),
                    'Std Dev': [None] * len(desired_dates),
                    'Open_PrevLow_Diff': [None] * len(desired_dates),
                    'Open_PrevLow_Diff_Percent': [None] * len(desired_dates),
                    'Open_PrevHigh_Diff': [None] * len(desired_dates),
                    'Open_PrevHigh_Diff_Percent': [None] * len(desired_dates),
                    'Open_Today_Close_Diff': [None] * len(desired_dates),
                })
                return empty_data
        else:
            # No data for the symbol
            empty_data = pd.DataFrame({
                'Symbol': [symbol] * len(desired_dates),
                'Date': [date.strftime("%Y-%m-%d") for date in date_objs],
                'Open Price': [None] * len(desired_dates),
                'Low Price': [None] * len(desired_dates),
                'High Price': [None] * len(desired_dates),
                'Close Price': [None] * len(desired_dates),
                'SMA': [None] * len(desired_dates),
                'Std Dev': [None] * len(desired_dates),
                'Open_PrevLow_Diff': [None] * len(desired_dates),
                'Open_PrevLow_Diff_Percent': [None] * len(desired_dates),
                'Open_PrevHigh_Diff': [None] * len(desired_dates),
                'Open_PrevHigh_Diff_Percent': [None] * len(desired_dates),
                'Open_Today_Close_Diff': [None] * len(desired_dates),
            })
            logging.warning(f"No data found for symbol {symbol}.")
            return empty_data
    except Exception as e:
        # Handle any other exceptions
        empty_data = pd.DataFrame({
            'Symbol': [symbol] * len(desired_dates),
            'Date': [date.strftime("%Y-%m-%d") for date in date_objs],
            'Open Price': [None] * len(desired_dates),
            'Low Price': [None] * len(desired_dates),
            'High Price': [None] * len(desired_dates),
            'Close Price': [None] * len(desired_dates),
            'SMA': [None] * len(desired_dates),
            'Std Dev': [None] * len(desired_dates),
            'Open_PrevLow_Diff': [None] * len(desired_dates),
            'Open_PrevLow_Diff_Percent': [None] * len(desired_dates),
            'Open_PrevHigh_Diff': [None] * len(desired_dates),
            'Open_PrevHigh_Diff_Percent': [None] * len(desired_dates),
            'Open_Today_Close_Diff': [None] * len(desired_dates),
        })
        logging.error(f"Error processing data for symbol {symbol}: {e}")
        return empty_data

def save_to_csv(df, output_file, selected_date_input):
    """
    Saves the DataFrame with Close prices, SMA, Std Dev, Open Price, Low Price,
    Open_PrevLow_Diff, Open_PrevLow_Diff_Percent, Open_PrevHigh_Diff, Open_PrevHigh_Diff_Percent,
    and Open_Today_Close_Diff to a CSV file.

    Parameters:
    - df (pandas DataFrame): DataFrame containing the data.
    - output_file (str): Path to the output CSV file.
    """
    try:
        df.to_csv(output_file, index=False)
        print(f"\nSuccessfully saved data to {output_file}")
        logging.info(f"Saved data to {output_file}")
    except Exception as e:
        print(f"Error saving to CSV: {e}")
        logging.error(f"Error saving to CSV {output_file}: {e}")

    # Identify unavailable tickers per date
    unavailable_df = df[df['Close Price'].isna()]
    if not unavailable_df.empty:
        unavailable_file = output_file.replace('.csv', f'_unavailable.csv')
        try:
            unavailable_df.to_csv(unavailable_file, index=False)
            print(f"Saved unavailable tickers to {unavailable_file}")
            logging.info(f"Saved unavailable tickers to {unavailable_file}")
        except Exception as e:
            print(f"Error saving unavailable tickers to CSV: {e}")
            logging.error(f"Error saving unavailable tickers to CSV {unavailable_file}: {e}")

def save_sorted_to_csv(df, selected_date, output_directory, PlaceOrderIK6635=True):
    """
    Saves sorted DataFrames based on Open_PrevLow_Diff_Percent and Open_PrevHigh_Diff_Percent for the selected date,
    filters stocks where the Open Price is higher than the 20-day SMA for long positions,
    and where Open Price is lower than the 20-day SMA for short positions.

    Parameters:
    - df (pandas DataFrame): The main DataFrame containing all data.
    - selected_date (str): The date for which sorting is to be performed ('YYYY-MM-DD').
    - output_directory (str): Directory where the sorted CSV will be saved.
    - PlaceOrderIK6635 (bool): Flag to determine whether to place orders. Defaults to True.

    Returns:
    - None
    """
    # Filter the DataFrame for the selected date
    print(df['Date'])
    df_selected_date = df[df['Date'] == selected_date]

    if df_selected_date.empty:
        print(f"\nNo data available for the selected date: {selected_date}.")
        logging.warning(f"No data available for the selected date: {selected_date}.")
        return

    # Drop rows where required fields are NaN to avoid sorting and filtering issues
    df_filtered = df_selected_date.dropna(subset=['Open_PrevLow_Diff_Percent', 'Open_PrevHigh_Diff_Percent', 'SMA'])

    if df_filtered.empty:
        print(f"\nAll entries for the selected date have NaN in required fields.")
        logging.warning(f"All entries for the selected date have NaN in required fields.")
        return

    # Filter for long positions: Open Price > SMA
    df_sorted = df_filtered[df_filtered['Open Price'] > df_filtered['SMA']]
    # Filter for short positions: Open Price < SMA
    df_sorted_short = df_filtered[df_filtered['Open Price'] < df_filtered['SMA']]

    print('Filtered details for Long Positions:')
    print(df_sorted)
    print('Filtered details for Short Positions:')
    print(df_sorted_short)

    if df_sorted.empty:
        print(f"\nNo stocks have Open Price higher than the 20-day SMA on {selected_date}.")
        logging.warning(f"No stocks have Open Price higher than the 20-day SMA on {selected_date}.")
    else:
        # Sort the DataFrame in ascending order based on 'Open_PrevLow_Diff_Percent'
        df_sorted = df_sorted.sort_values(by='Open_PrevLow_Diff_Percent', ascending=True)

    if df_sorted_short.empty:
        print(f"\nNo stocks have Open Price lower than the 20-day SMA on {selected_date}.")
        logging.warning(f"No stocks have Open Price lower than the 20-day SMA on {selected_date}.")
    else:
        # Sort the DataFrame in descending order based on 'Open_PrevHigh_Diff_Percent'
        df_sorted_short = df_sorted_short.sort_values(by='Open_PrevHigh_Diff_Percent', ascending=False)

    # Get today's date as a string in 'YYYY-MM-DD' format
    today_str = datetime.today().strftime('%Y-%m-%d')
    print(today_str)
    # Define the sorted output file paths with today's date in the filename
    sorted_output_file = os.path.join(output_directory, f'close_prices_sorted_long_{selected_date}.csv')
    sorted_output_file_short = os.path.join(output_directory, f'close_prices_sorted_short_{selected_date}.csv')

    # Save the sorted DataFrames to CSV files
    if not df_sorted.empty:
        try:
            df_sorted.to_csv(sorted_output_file, index=False)
            print(f"\nSuccessfully saved sorted data to {sorted_output_file}")
            logging.info(f"Saved sorted data to {sorted_output_file}")
        except Exception as e:
            print(f"Error saving sorted data to CSV: {e}")
            logging.error(f"Error saving sorted data to CSV {sorted_output_file}: {e}")

    if not df_sorted_short.empty:
        try:
            df_sorted_short.to_csv(sorted_output_file_short, index=False)
            print(f"\nSuccessfully saved sorted data to {sorted_output_file_short}")
            logging.info(f"Saved sorted data to {sorted_output_file_short}")
        except Exception as e:
            print(f"Error saving sorted data to CSV: {e}")
            logging.error(f"Error saving sorted data to CSV {sorted_output_file_short}: {e}")

    # Optionally, print the sorted DataFrames
    # print("\nSorted Tickers for Long Positions:")
    # print(df_sorted)
    # print("\nSorted Tickers for Short Positions:")
    # print(df_sorted_short)

    # Call Function to place orders on Zerodha account IK6635 if flag is True
    if SendFileDataByEmail:
        
        PayloadDataFrames = {f'IntradayLong{selected_date_input}.csv':df_sorted, f'IntradayShort{selected_date_input}.csv':df_sorted_short}
        SendConfigurableMail(PayloadDataFrames, VMailDetails)
        SendConfigurableMail(PayloadDataFrames, DadMailDetails)
        SendConfigurableMail(PayloadDataFrames, EkanshMailDetails)

    # Call Function to place orders on Zerodha account IK6635 if flag is True
    if PlaceOrderIK6635:
        while True:
            now_str = datetime.now().strftime("%H:%M:%S")
            if now_str == OrderTriggerTime:
                print("It's 09:15:00. Proceeding...")
                trade_type_1, trade_type_2 = determine_trade_type()
                PlaceIntradayOrders(df_sorted, df_sorted_short, trade_type_1, trade_type_2)
                break
            else:
                # Sleep for a short interval to avoid busy-waiting
                print(f'waiting, current time is {now_str}')
                time.sleep(1)

def determine_trade_type():
    """
    Determines the trade type based on the current time.
    - Before 11 AM: 'BUY' (long)
    - After 11 AM: 'SELL' (short)

    Returns:
    - str: 'BUY' or 'SELL' for long positions
    - str: 'SELL' or 'BUY' for short positions
    """
    current_time = datetime.now().time()
    eleven_am = datetime.strptime("18:00:00", "%H:%M:%S").time()

    if current_time < eleven_am:
        trade_type_1 = 'BUY'
        trade_type_2 = 'SELL'
    else:
        trade_type_1 = 'SELL'
        trade_type_2 = 'BUY'

    logging.info(f"Determined trade type for long positions: {trade_type_1} based on current time: {current_time}")
    print(f"Determined trade type for long positions: {trade_type_1} based on current time: {current_time}")

    logging.info(f"Determined trade type for short positions: {trade_type_2} based on current time: {current_time}")
    print(f"Determined trade type for short positions: {trade_type_2} based on current time: {current_time}")

    return trade_type_1, trade_type_2

def main():
    # Define the path to the CSV file
    csv_file_path = Nifty500ConstituentList

    # Attempt to read as comma-separated
    df = read_csv_file(csv_file_path, delimiter=',')

    # Check if 'Symbol' column exists
    if 'Symbol' in df.columns:
        symbols = df['Symbol'].dropna().unique().tolist()
        print(f"Total symbols found: {len(symbols)}")
    else:
        # If 'Symbol' column not found, try reading with tab delimiter
        print("Attempting to read the CSV file with tab delimiter...")
        df = read_csv_file(csv_file_path, delimiter='\t')
        if 'Symbol' in df.columns:
            symbols = df['Symbol'].dropna().unique().tolist()
            print(f"Total symbols found: {len(symbols)}")
        else:
            # Handle cases where header might be a single column with comma-separated values
            if len(df.columns) == 1:
                # Split the single column into multiple columns
                new_columns = df.columns[0].split(',')
                df.columns = new_columns
                print(f"Reformatted columns: {df.columns.tolist()}")
                if 'Symbol' in df.columns:
                    symbols = df['Symbol'].dropna().unique().tolist()
                    print(f"Total symbols found: {len(symbols)}")
                else:
                    print("Error: 'Symbol' column not found in the CSV file.")
                    logging.error("Symbol column not found after reformatting.")
                    exit(1)
            else:
                print("Error: 'Symbol' column not found in the CSV file.")
                logging.error("Symbol column not found in the CSV file.")
                exit(1)

    # Prompt the user to input the selected date, lookback period, SMA window, and Std Dev window
    print("\nEnter the selected date for which you want to fetch the Close prices.")
    print("Enter the date in 'YYYY-MM-DD' format (e.g., 2024-10-21):")

    # print("\nEnter the lookback period (number of trading days to look back from the selected date).")
    # print("For example, enter 90 to fetch data from 90 trading days prior to the selected date:")
    lookback_input = '120'  # input("Lookback Period (trading days): ").strip() or '120'

    # print("\nEnter the SMA window (number of days for Simple Moving Average):")
    sma_input = '20'  # input("SMA Window (days): ").strip() or '20'

    # print("\nEnter the Std Dev window (number of days for Standard Deviation):")
    std_dev_input = '90'  # input("Std Dev Window (days): ").strip() or '90'

    # Validate the selected date
    try:
        selected_date_obj = datetime.strptime(selected_date_input, "%Y-%m-%d")
    except ValueError:
        print("Error: Selected date is not in the correct format 'YYYY-MM-DD'.")
        logging.error(f"Incorrect date format: {selected_date_input}")
        exit(1)

    # Validate the lookback period
    try:
        lookback_period = int(lookback_input)
        if lookback_period < 1:
            raise ValueError
    except ValueError:
        print("Error: Lookback period must be a positive integer.")
        logging.error(f"Invalid lookback period: {lookback_input}")
        exit(1)

    # Validate the SMA window
    try:
        sma_window = int(sma_input)
        if sma_window < 1:
            raise ValueError
    except ValueError:
        print("Error: SMA window must be a positive integer.")
        logging.error(f"Invalid SMA window: {sma_input}")
        exit(1)

    # Validate the Std Dev window
    try:
        std_dev_window = int(std_dev_input)
        if std_dev_window < 1:
            raise ValueError
    except ValueError:
        print("Error: Std Dev window must be a positive integer.")
        logging.error(f"Invalid Std Dev window: {std_dev_input}")
        exit(1)

    # Ensure that lookback_period is at least as large as the maximum of sma_window and std_dev_window
    required_period = max(sma_window, std_dev_window)
    if lookback_period < required_period:
        print(f"Adjusting lookback period from {lookback_period} to {required_period} to accommodate SMA and Std Dev windows.")
        logging.info(f"Adjusted lookback period from {lookback_period} to {required_period}")
        lookback_period = required_period

    # Calculate the start date by subtracting the lookback period (in business days) from the selected date
    # Note: This does not account for market holidays specific to NSE
    # For more accurate trading days, consider using a market calendar
    start_date_obj = selected_date_obj - timedelta(days=lookback_period * 2)  # Over-approximation
    trading_days = pd.bdate_range(end=selected_date_obj, periods=lookback_period + 1).strftime("%Y-%m-%d").tolist()
    # The +1 includes the selected_date itself
    print(f"\nSelected Date: {selected_date_input}")
    print(f"Lookback Period: {lookback_period} trading days")
    print(f"SMA Window: {sma_window} days")
    print(f"Std Dev Window: {std_dev_window} days")
    print(f"Fetching Close prices from {trading_days[0]} to {trading_days[-1]}")

    # Fetch Close prices in batches and compute SMA, Std Dev, Open_PrevLow_Diff Percent, Open_Today_Close_Diff
    df_close = fetch_ltp(symbols, trading_days, sma_window, std_dev_window, batch_size=total_batch_size, pause=1)

    # Prepare the output
    output_directory = IntraDayDirectory
    output_file = os.path.join(output_directory, f"close_prices_individual_{selected_date_input}.csv")

    # Save the results to a CSV file
    save_to_csv(df_close, output_file, selected_date_input)

    # Create and save the sorted DataFrame based on 'Open_PrevLow_Diff_Percent' for the selected date
    save_sorted_to_csv(df_close, selected_date_input, output_directory)

    # Optionally, print the results
    '''print("\nAvailable Tickers with Close Prices, SMA, Std Dev, Open Price, Low Price, and Open_PrevLow_Diff:")
    available_df = df_close[df_close['Close Price'].notna()]
    if not available_df.empty:
        print(available_df)
    else:
        print("No available tickers with Close Prices.")'''

    # Optionally, print unavailable symbols
    unavailable_df = df_close[df_close['Close Price'].isna()]
    if not unavailable_df.empty:
        print("\nSome Tickers have No Data Available:")
        # print(unavailable_df)
    else:
        print("\nAll tickers have Close Prices available.")

if __name__ == "__main__":
    main()
