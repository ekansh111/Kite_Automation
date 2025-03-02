"""
This script is designed to fetch historical stock data for a list of symbols from the Nifty 500 constituents,
compute technical indicators, filter stocks based on specific criteria, and optionally place intraday orders.

Main Functionalities:

1. Read Stock Symbols:
   - Reads a CSV file containing stock symbols (Nifty500ConstituentList).

2. Fetch Historical Data:
   - Uses the yfinance library to download historical Open, Low, High, and Close prices for the specified symbols
     over a defined date range.
   - Fetches data in batches to avoid overwhelming the API.

3. Compute Technical Indicators:
   - Simple Moving Average (SMA): Calculated over a specified window (default is 20 days).
   - Standard Deviation (Std Dev): Calculated over a specified window (default is 90 days).
   - Other technical calculations (differences and percentages between open, previous low/high, and close) are performed.

4. Data Saving:
   - Saves the fetched data and computed indicators to CSV files.
   - Saves sorted data based on Open_PrevLow_Diff_Percent and Open_PrevHigh_Diff_Percent.
   - Creates target results DataFrames for long and short positions.

5. Filtering Stocks:
   - Filters stocks where the Open Price is higher than the 20-day SMA (for potential long positions)
     and where it is lower than the 20-day SMA (for potential short positions).

6. Place Intraday Orders (Optional):
   - Determines trade type based on current time (before 11 AM: BUY, after 11 AM: SELL).
   - Places intraday orders if enabled by the PlaceOrderIK6635 flag.

7. Logging and Error Handling:
   - Uses logging to capture warnings and errors.
   - Handles exceptions during data fetching and processing.
   - Logs missing data and other issues.

8. Multiprocessing:
   - Uses multiprocessing to speed up processing for multiple symbols.

Notes:
- The script is tailored for the NSE, appending ".NS" to stock symbols.
- Directory and file paths (e.g. IntraDayDirectory, Nifty500ConstituentList) are imported from the Directories module.
- Ensure all required modules (pandas, yfinance, etc.) are installed.
"""

import os
from inputimeout import inputimeout, TimeoutOccurred
import pandas as pd
import time
import logging
import yfinance as yf
from itertools import islice
from datetime import datetime, timedelta
from IntraDay_Stocks_Place_Order import *
from multiprocessing import Pool, cpu_count
from Directories import *  # Contains IntraDayDirectory, Nifty500ConstituentList, etc.
from Push_File_To_Email import *
from Email_Config import *
import psycopg2  # For fallback from dailybhavcopy
import warnings

warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")
warnings.filterwarnings("ignore", category=FutureWarning, message="Setting an item of incompatible dtype is deprecated")
warnings.filterwarnings("ignore", category=FutureWarning, message="The behavior of DataFrame concatenation with empty or all-NA entries is deprecated")

# Global constants
TotalBatchSize = 500
PlaceOrderIK6635 = True  # Flag to decide if orders are to be placed
CommissionPercent = 0.3
FixedCommissionCost = 0
StopLossAbsValue = 0.99

# =============================================================================
# Utility Functions
# =============================================================================

def ReadCsvFile(filePath, delimiter=","):
    try:
        df = pd.read_csv(filePath, delimiter=delimiter)
        print(f"Successfully read the CSV file: {filePath}")
        print(f"Columns Found: {df.columns.tolist()}")
        return df
    except FileNotFoundError:
        print(f"Error: The file {filePath} does not exist.")
        logging.error(f"File not found: {filePath}")
        exit(1)
    except pd.errors.ParserError as e:
        print(f"Error: Failed to parse the CSV file. {e}")
        logging.error(f"Parser error for file {filePath}: {e}")
        exit(1)

def BatchIterator(iterable, batchSize):
    it = iter(iterable)
    while True:
        batch = list(islice(it, batchSize))
        if not batch:
            break
        yield batch

# =============================================================================
# Data Fetching Functions
# =============================================================================

def FetchLtp(symbols, dates, smaWindow, stdDevWindow, batchSize, pause=1):
    closePrices = []
    logging.basicConfig(
        filename='fetch_close_prices.log',
        filemode='a',
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.WARNING
    )
    totalBatches = (len(symbols) + batchSize - 1) // batchSize
    dateObjs = sorted([datetime.strptime(date, "%Y-%m-%d") for date in dates])
    if not dateObjs:
        logging.error("No valid dates provided.")
        exit(1)
    startDate = dateObjs[0]
    endDate = dateObjs[-1] + timedelta(days=1)  # Include the last day
    startStr = startDate.strftime("%Y-%m-%d")
    endStr = endDate.strftime("%Y-%m-%d")
    #print(f'end string {endStr}')
    desiredDates = set(date.strftime("%Y-%m-%d") for date in dateObjs)

    for idx, batch in enumerate(BatchIterator(symbols, batchSize), start=1):
        symbolsWithSuffix = [symbol + ".NS" for symbol in batch]
        try:
            print(f'Beginning the download for batch {idx}/{totalBatches}')
            data = yf.download(
                tickers=symbolsWithSuffix,
                start=startStr,
                end=endStr,
                interval="1d",
                group_by='ticker',
                threads=True,
                progress=False
            )
            #print('data from yfinance')
            #print(data)
        except Exception as e:
            logging.error(f"Error fetching data for batch {idx}: {e}")
            for symbol in batch:
                emptyData = pd.DataFrame({
                    'Symbol': [symbol] * len(desiredDates),
                    'Date': [d.strftime("%Y-%m-%d") for d in dateObjs],
                    'Open Price': [None] * len(desiredDates),
                    'Low Price': [None] * len(desiredDates),
                    'High Price': [None] * len(desiredDates),
                    'Close Price': [None] * len(desiredDates),
                    'SMA': [None] * len(desiredDates),
                    'Std Dev': [None] * len(desiredDates),
                    'Open_PrevLow_Diff': [None] * len(desiredDates),
                    'Open_PrevLow_Diff_Percent': [None] * len(desiredDates),
                    'Open_PrevHigh_Diff': [None] * len(desiredDates),
                    'Open_PrevHigh_Diff_Percent': [None] * len(desiredDates),
                    'Open_Today_Close_Diff': [None] * len(desiredDates),
                })
                closePrices.append(emptyData)
            time.sleep(pause)
            continue

        if len(batch) == 1 and data.columns.nlevels == 1:
            data.columns = pd.MultiIndex.from_product([[symbolsWithSuffix[0]], data.columns])
        argsList = [(symbol, data, desiredDates, dateObjs, smaWindow, stdDevWindow) for symbol in batch]
        with Pool(processes=min(cpu_count(), len(batch))) as pool:
            results = pool.map(process_symbol_data, argsList)
            closePrices.extend(results)
        time.sleep(pause)

    dfClose = pd.concat(closePrices, ignore_index=True)
    dfClose = dfClose.drop_duplicates(subset=['Symbol', 'Date'])
    return dfClose

def FetchFromDailyBhavcopy(symbol, startDate, endDate):
    print(f"Fetch from bhavcopy function is called for symbol {symbol}")
    connection = None
    try:
        connection = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="1812",
            host="localhost",
            port=5432
        )
        sql = f"""
            SELECT recdate as "Date", open as "Open", high as "High",
                   low as "Low", close as "Close"
            FROM dailybhavcopy
            WHERE symbol = '{symbol}'
              AND recdate >= '{startDate.strftime("%Y-%m-%d")}'
              AND recdate <= '{endDate.strftime("%Y-%m-%d")}'
              AND series = 'EQ'
            ORDER BY recdate;
        """
        df = pd.read_sql(sql, connection)

        if not df.empty:
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)
        else:
            print(f"No fallback data fetched for symbol: {symbol} ")    
        return df
    except Exception as e:
        print(f"Error fetching fallback data from dailybhavcopy table for {symbol}: {e}")
        logging.error(f"Fallback data fetch error for {symbol}: {e}")
        return pd.DataFrame()
    finally:
        if connection:
            connection.close()

# =============================================================================
# Calculation Functions
# =============================================================================

def ComputeStopAndDiff(row, slPercent):
    requiredCols = ['Open', 'Close', 'SMA', 'Low', 'High']
    for col in requiredCols:
        if col not in row:
            logging.error(f"Missing column {col} in row for symbol {row.get('Symbol', 'Unknown')}.")
            return pd.Series({'Stop Price': None, 'Open_Today_Close_Diff': None})
    try:
        if pd.isna(row['Open']) or pd.isna(row['SMA']):
            return pd.Series({'Stop Price': None, 'Open_Today_Close_Diff': None})
        openPrice = row['Open']
        closePrice = row['Close']
        smaValue = row['SMA']
        lowPrice = row['Low']
        highPrice = row['High']
        stopPrice = None
        openTodayCloseDiff = None
        if openPrice > smaValue:
            stopPrice = openPrice - (openPrice * slPercent)
            if lowPrice < stopPrice:
                openTodayCloseDiff = (stopPrice - openPrice) / openPrice * 100
            else:
                openTodayCloseDiff = (closePrice - openPrice) / openPrice * 100
        elif openPrice < smaValue:
            stopPrice = openPrice + (openPrice * slPercent)
            if highPrice > stopPrice:
                openTodayCloseDiff = (stopPrice - openPrice) / openPrice * 100
            else:
                openTodayCloseDiff = (closePrice - openPrice) / openPrice * 100
        else:
            stopPrice = None
            openTodayCloseDiff = (closePrice - openPrice) / openPrice * 100
        result = pd.Series({'Stop Price': stopPrice, 'Open_Today_Close_Diff': openTodayCloseDiff})
        if len(result) != 2:
            raise ValueError("Output series does not have exactly 2 columns.")
        return result
    except Exception as ex:
        logging.error(f"Exception in ComputeStopAndDiff for row: {ex}")
        return pd.Series({'Stop Price': None, 'Open_Today_Close_Diff': None})

def process_symbol_data(args):
    symbol, data, desiredDates, dateObjs, smaWindow, stdDevWindow = args
    symbolWithSuffix = symbol + ".NS"
    final_cols = ['Symbol', 'Date', 'Open Price', 'Low Price', 'High Price', 'Close Price',
                  'SMA', 'Std Dev', 'Open_PrevLow_Diff', 'Open_PrevLow_Diff_Percent',
                  'Open_PrevHigh_Diff', 'Open_PrevHigh_Diff_Percent',
                  'Stop Price', 'Open_Today_Close_Diff']
    try:
        if symbolWithSuffix in data.columns.levels[0]:
            tickerData = data[symbolWithSuffix].copy()
        else:
            tickerData = pd.DataFrame()

        if not tickerData.empty:
            if not isinstance(tickerData.index, pd.DatetimeIndex):
                tickerData.index = pd.to_datetime(tickerData.index, errors='coerce')
                if tickerData.index.isnull().all():
                    raise ValueError("All dates could not be converted to datetime.")
            tickerData = tickerData.sort_index().dropna()
            if tickerData.empty:
                raise ValueError(f"No valid data available for {symbol} after dropping NAs.")

            # Compute technical indicators
            tickerData['SMA'] = tickerData['Close'].rolling(window=smaWindow).mean().shift(1)
            tickerData['Std Dev'] = tickerData['Close'].rolling(window=stdDevWindow).std().shift(1)
            tickerData['Prev_Low'] = tickerData['Low'].shift(1)
            tickerData['Prev_High'] = tickerData['High'].shift(1)
            tickerData['Open_PrevLow_Diff'] = tickerData['Open'] - tickerData['Prev_Low']
            tickerData['Open_PrevLow_Diff_Percent'] = (tickerData['Open_PrevLow_Diff'] / tickerData['Prev_Low']) * 100
            tickerData['Open_PrevHigh_Diff'] = tickerData['Open'] - tickerData['Prev_High']
            tickerData['Open_PrevHigh_Diff_Percent'] = (tickerData['Open_PrevHigh_Diff'] / tickerData['Prev_High']) * 100
            tickerData['Open_PrevLow_Diff_Percent'] = tickerData['Open_PrevLow_Diff_Percent'].replace([float('inf'), -float('inf')], pd.NA)
            tickerData['Open_PrevHigh_Diff_Percent'] = tickerData['Open_PrevHigh_Diff_Percent'].replace([float('inf'), -float('inf')], pd.NA)
            tickerData['Open_Today_Close_Diff'] = ((tickerData['Close'] - tickerData['Open']) / tickerData['Open']) * 100
            tickerData['Date'] = tickerData.index.strftime("%Y-%m-%d")
            tickerData = tickerData[tickerData['Date'].isin(desiredDates)]
            if tickerData.empty:
                raise ValueError(f"No data available for {symbol} on desired dates after processing.")
            
            # Drop the pre-computed 'Open_Today_Close_Diff' so that it can be replaced by the computed value.
            if 'Open_Today_Close_Diff' in tickerData.columns:
                tickerData.drop(columns=['Open_Today_Close_Diff'], inplace=True)
            
            # Compute Stop Price and revised Open_Today_Close_Diff using the custom function.
            stopDiff = tickerData.apply(lambda r: ComputeStopAndDiff(r, StopLossAbsValue), axis=1)
            tickerData = pd.concat([tickerData, stopDiff], axis=1)
            
            # Rename price columns to expected names.
            tickerData.rename(columns={
                'Open': 'Open Price',
                'High': 'High Price',
                'Low': 'Low Price',
                'Close': 'Close Price'
            }, inplace=True)
        else:
            tickerData = pd.DataFrame({
                'Symbol': [symbol] * len(desiredDates),
                'Date': [d.strftime("%Y-%m-%d") for d in dateObjs],
                'Open Price': [None] * len(desiredDates),
                'Low Price': [None] * len(desiredDates),
                'High Price': [None] * len(desiredDates),
                'Close Price': [None] * len(desiredDates),
                'SMA': [None] * len(desiredDates),
                'Std Dev': [None] * len(desiredDates),
                'Open_PrevLow_Diff': [None] * len(desiredDates),
                'Open_PrevLow_Diff_Percent': [None] * len(desiredDates),
                'Open_PrevHigh_Diff': [None] * len(desiredDates),
                'Open_PrevHigh_Diff_Percent': [None] * len(desiredDates),
                'Open_Today_Close_Diff': [None] * len(desiredDates),
            })
            logging.warning(f"No data found for symbol {symbol}.")
            return tickerData

        tickerData['Symbol'] = symbol
        tickerData = tickerData.reset_index(drop=True)
        tickerData = tickerData.loc[:, ~tickerData.columns.str.contains('^Unnamed')]
        
        # Ensure that all final columns exist
        for col in final_cols:
            if col not in tickerData.columns:
                tickerData[col] = pd.NA
        tickerData = tickerData[final_cols]
        return tickerData

    except Exception as e:
        logging.error(f"Error processing data for symbol {symbol} from yfinance: {e}")
        print(f"Yfinance data fetch/process failed for {symbol}: {e}. Trying fallback from dailybhavcopy.")
        
        startDate = dateObjs[0]
        endDate = dateObjs[-1]
        tickerData = FetchFromDailyBhavcopy(symbol, startDate, endDate)
        if tickerData.empty or not all(col in tickerData.columns for col in ['Open', 'High', 'Low', 'Close']):
            logging.error(f"No fallback data found for symbol {symbol}.")
            return MakeEmptyDataFrame(symbol, desiredDates)
        tickerData = tickerData.sort_index().dropna()
        if tickerData.empty:
            logging.error(f"Fallback data for {symbol} is empty after dropna().")
            return MakeEmptyDataFrame(symbol, desiredDates)
        tickerData['SMA'] = tickerData['Close'].rolling(window=smaWindow).mean().shift(1)
        tickerData['Std Dev'] = tickerData['Close'].rolling(window=stdDevWindow).std().shift(1)
        tickerData['Prev_Low'] = tickerData['Low'].shift(1)
        tickerData['Prev_High'] = tickerData['High'].shift(1)
        tickerData['Open_PrevLow_Diff'] = tickerData['Open'] - tickerData['Prev_Low']
        tickerData['Open_PrevLow_Diff_Percent'] = (tickerData['Open_PrevLow_Diff'] / tickerData['Prev_Low']) * 100
        tickerData['Open_PrevHigh_Diff'] = tickerData['Open'] - tickerData['Prev_High']
        tickerData['Open_PrevHigh_Diff_Percent'] = (tickerData['Open_PrevHigh_Diff'] / tickerData['Prev_High']) * 100
        tickerData['Open_PrevLow_Diff_Percent'] = tickerData['Open_PrevLow_Diff_Percent'].replace([float('inf'), -float('inf')], pd.NA)
        tickerData['Open_PrevHigh_Diff_Percent'] = tickerData['Open_PrevHigh_Diff_Percent'].replace([float('inf'), -float('inf')], pd.NA)
        tickerData['Date'] = tickerData.index.strftime("%Y-%m-%d")
        tickerData = tickerData[tickerData['Date'].isin(desiredDates)]
        stopDiff = tickerData.apply(lambda r: ComputeStopAndDiff(r, StopLossAbsValue), axis=1)
        tickerData = pd.concat([tickerData, stopDiff], axis=1)

    if not tickerData.empty:
        tickerData['Symbol'] = symbol
        tickerData = tickerData.reset_index(drop=True)
        tickerData = tickerData[[ 
            'Symbol', 'Date', 'Open', 'Low', 'High', 'Close', 'SMA', 'Std Dev',
            'Open_PrevLow_Diff', 'Open_PrevLow_Diff_Percent',
            'Open_PrevHigh_Diff', 'Open_PrevHigh_Diff_Percent',
            'Stop Price', 'Open_Today_Close_Diff'
        ]]
        tickerData.columns = [
            'Symbol', 'Date', 'Open Price', 'Low Price', 'High Price', 'Close Price',
            'SMA', 'Std Dev', 'Open_PrevLow_Diff', 'Open_PrevLow_Diff_Percent',
            'Open_PrevHigh_Diff', 'Open_PrevHigh_Diff_Percent',
            'Stop Price', 'Open_Today_Close_Diff'
        ]
        return tickerData
    else:
        logging.warning(f"No data found for symbol {symbol}.")
        # Fallback processing omitted for brevity
        return MakeEmptyDataFrame(symbol, desiredDates)


def MakeEmptyDataFrame(symbol, desiredDates):
    if not isinstance(desiredDates, list):
        desiredDates = sorted(list(desiredDates))
    return pd.DataFrame({
        'Symbol': [symbol] * len(desiredDates),
        'Date': desiredDates,
        'Open Price': [None] * len(desiredDates),
        'Low Price': [None] * len(desiredDates),
        'High Price': [None] * len(desiredDates),
        'Close Price': [None] * len(desiredDates),
        'SMA': [None] * len(desiredDates),
        'Std Dev': [None] * len(desiredDates),
        'Open_PrevLow_Diff': [None] * len(desiredDates),
        'Open_PrevLow_Diff_Percent': [None] * len(desiredDates),
        'Open_PrevHigh_Diff': [None] * len(desiredDates),
        'Open_PrevHigh_Diff_Percent': [None] * len(desiredDates),
        'Open_Today_Close_Diff': [None] * len(desiredDates),
    })

# =============================================================================
# Saving Functions
# =============================================================================

def SaveToCsv(df, outputFile, selectedDateInput):
    try:
        df.to_csv(outputFile, index=False)
        print(f"\nSuccessfully saved data to {outputFile}")
        logging.info(f"Saved data to {outputFile}")
    except Exception as e:
        print(f"Error saving to CSV: {e}")
        logging.error(f"Error saving to CSV {outputFile}: {e}")

    unavailable_df = df[df['Close Price'].isna()]
    if not unavailable_df.empty:
        unavailable_file = outputFile.replace('.csv', f'_unavailable.csv')
        try:
            unavailable_df.to_csv(unavailable_file, index=False)
            print(f"Saved unavailable tickers to {unavailable_file}")
            logging.info(f"Saved unavailable tickers to {unavailable_file}")
        except Exception as e:
            print(f"Error saving unavailable tickers to CSV: {e}")
            logging.error(f"Error saving unavailable tickers to CSV {unavailable_file}: {e}")

def SaveSortedToCsv(df, selectedDate, outputDirectory, targetResultsLongDf, targetResultsShortDf):
    dfSelectedDate = df[df['Date'] == selectedDate]
    if dfSelectedDate.empty:
        print(f"\nNo data available for the selected date: {selectedDate}.")
        logging.warning(f"No data available for the selected date: {selectedDate}.")
        return targetResultsLongDf, targetResultsShortDf

    dfFiltered = dfSelectedDate.dropna(subset=['Open_PrevLow_Diff_Percent', 'Open_PrevHigh_Diff_Percent', 'SMA', 'Open_Today_Close_Diff'])
    if dfFiltered.empty:
        print(f"\nAll entries for the selected date have NaN in required fields.")
        logging.warning("All entries for the selected date have NaN in required fields.")
        return targetResultsLongDf, targetResultsShortDf

    #print('df filtered:')
    #print(dfFiltered)

    dfSorted = dfFiltered[dfFiltered['Open Price'] > dfFiltered['SMA']]
    dfSortedShort = dfFiltered[dfFiltered['Open Price'] < dfFiltered['SMA']]
    #print("df sorted (long candidates):")
    #print(dfSorted)
    #print("df sorted short (short candidates):")
    #print(dfSortedShort)

    if not dfSorted.empty:
        dfSorted = dfSorted.sort_values(by='Open_PrevLow_Diff_Percent', ascending=True)
    else:
        print(f"\nNo stocks have Open Price higher than the 20-day SMA on {selectedDate}.")
        logging.warning(f"No stocks have Open Price higher than the 20-day SMA on {selectedDate}.")

    if not dfSortedShort.empty:
        dfSortedShort = dfSortedShort.sort_values(by='Open_PrevHigh_Diff_Percent', ascending=False)
    else:
        print(f"\nNo stocks have Open Price lower than the 20-day SMA on {selectedDate}.")
        logging.warning(f"No stocks have Open Price lower than the 20-day SMA on {selectedDate}.")

    next2DayDateObj = datetime.strptime(selectedDate, "%Y-%m-%d") + timedelta(days=2)
    searchLimit = 365
    forwardDateObj = next2DayDateObj
    df2Day = pd.DataFrame(columns=['Symbol', 'forward_2_day_close'])
    daysSearched = 0
    while daysSearched < searchLimit:
        forwardDate = forwardDateObj.strftime("%Y-%m-%d")
        df2DayCandidate = df[df['Date'] == forwardDate][['Symbol', 'Close Price']].copy()
        if not df2DayCandidate.empty:
            df2Day = df2DayCandidate.rename(columns={'Close Price': 'forward_2_day_close'})
            break
        else:
            forwardDateObj += timedelta(days=1)
            daysSearched += 1

    if not df2Day.empty:
        if not dfSorted.empty:
            dfSorted = dfSorted.merge(df2Day, on='Symbol', how='left')
        if not dfSortedShort.empty:
            dfSortedShort = dfSortedShort.merge(df2Day, on='Symbol', how='left')
    else:
        if not dfSorted.empty:
            dfSorted['forward_2_day_close'] = pd.NA
        if not dfSortedShort.empty:
            dfSortedShort['forward_2_day_close'] = pd.NA

    if not dfSorted.empty:
        longCondition = (dfSorted['Low Price'] == dfSorted['Close Price']) & (dfSorted['forward_2_day_close'].notna())
        dfSorted.loc[longCondition, 'Open_Today_Close_Diff'] = (
            (dfSorted.loc[longCondition, 'forward_2_day_close'] - dfSorted.loc[longCondition, 'Open Price']) /
            dfSorted.loc[longCondition, 'Open Price']
        ) * 100
    if not dfSortedShort.empty:
        shortCondition = (dfSortedShort['High Price'] == dfSortedShort['Close Price']) & (dfSortedShort['forward_2_day_close'].notna())
        dfSortedShort.loc[shortCondition, 'Open_Today_Close_Diff'] = (
            (dfSortedShort.loc[shortCondition, 'Open Price'] - dfSortedShort.loc[shortCondition, 'forward_2_day_close']) /
            dfSortedShort.loc[shortCondition, 'Open Price']
        ) * 100 * -1

    # Process long positions
    if not dfSorted.empty:
        dfLongTop = dfSorted.head(NumberOfStocksToSelectLowestOpenPrice)
        averageReturnLong = dfLongTop['Open_Today_Close_Diff'].mean()
        absoluteReturnPoints = (dfSorted['Open Price'] * dfSorted['Open_Today_Close_Diff']) / 100 
        averageAbsolutePnl = (dfSorted['Open_Today_Close_Diff'] * CapitalRiskedPerLongTrade) / 100
        averageStdDevAdjPnl = absoluteReturnPoints * (TargetVolatilityPerLongTrade / dfSorted['Std Dev'])
        absoluteReturnPointsSelected = ((dfLongTop['Open Price'] * dfLongTop['Open_Today_Close_Diff']) / 100) - ((dfLongTop['Open Price'] * CommissionPercent) / 100)
        averageStdDevAdjPnlSelected = absoluteReturnPointsSelected * (TargetVolatilityPerLongTrade / dfLongTop['Std Dev'])
        dfSorted['stdDevAdjQuantity'] = TargetVolatilityPerLongTrade / dfSorted['Std Dev']
        dfSorted['stdDevAdjustedPnl'] = averageStdDevAdjPnl
        dfSorted['avgAbsolutePnl'] = averageAbsolutePnl
        cumTotalAdjStdDevPnl = averageStdDevAdjPnlSelected.sum() - FixedCommissionCost
        newRowLong = {'date': selectedDate, 'returns': averageReturnLong, 'stddevAdjPnl': cumTotalAdjStdDevPnl}
        targetResultsLongDf = pd.concat([targetResultsLongDf, pd.DataFrame([newRowLong])], ignore_index=True)
    else:
        print(f"\nNo long positions to calculate target results for {selectedDate}.")

    # Process short positions
    if not dfSortedShort.empty:
        dfShortTop = dfSortedShort.head(NumberOfStocksToSelectHighestOpenPrice)
        averageReturnShort = dfShortTop['Open_Today_Close_Diff'].mean()
        absoluteReturnPointsShort = ((dfSortedShort['Open Price'] * dfSortedShort['Open_Today_Close_Diff']) / 100) * -1
        averageAbsolutePnlShort = (dfSortedShort['Open_Today_Close_Diff'] * (CapitalRiskedPerShortTrade * -1)) / 100
        averageStdDevAdjPnlShort = absoluteReturnPointsShort * (TargetVolatilityPerShortTrade / dfSortedShort['Std Dev'])
        absoluteReturnPointsSelectedShort = (((dfShortTop['Open Price'] * dfShortTop['Open_Today_Close_Diff']) / 100) * -1) - ((dfShortTop['Open Price'] * CommissionPercent) / 100)
        averageStdDevAdjPnlSelectedShort = absoluteReturnPointsSelectedShort * (TargetVolatilityPerShortTrade / dfShortTop['Std Dev'])
        dfSortedShort['stdDevAdjQuantity'] = TargetVolatilityPerShortTrade / dfSortedShort['Std Dev']
        dfSortedShort['stdDevAdjustedPnlNoComm'] = averageStdDevAdjPnlShort
        dfSortedShort['avgAbsolutePnl'] = averageAbsolutePnlShort
        cumTotalAdjStdDevPnlShort = averageStdDevAdjPnlSelectedShort.sum() - FixedCommissionCost
        newRowShort = {'date': selectedDate, 'returns': averageReturnShort, 'stddevAdjPnl': cumTotalAdjStdDevPnlShort}
        targetResultsShortDf = pd.concat([targetResultsShortDf, pd.DataFrame([newRowShort])], ignore_index=True)
    else:
        print(f"\nNo short positions to calculate target results for {selectedDate}.")

    sortedOutputFile = os.path.join(outputDirectory, f'close_prices_sorted_long_{selectedDate}.csv')
    sortedOutputFileShort = os.path.join(outputDirectory, f'close_prices_sorted_short_{selectedDate}.csv')
    if not dfSorted.empty:
        try:
            dfSorted.to_csv(sortedOutputFile, index=False)
            print(f"\nSuccessfully saved sorted data to {sortedOutputFile}")
            logging.info(f"Saved sorted data to {sortedOutputFile}")
        except Exception as e:
            print(f"Error saving sorted data to CSV: {e}")
            logging.error(f"Error saving sorted data to CSV {sortedOutputFile}: {e}")
    if not dfSortedShort.empty:
        try:
            dfSortedShort.to_csv(sortedOutputFileShort, index=False)
            print(f"\nSuccessfully saved sorted data to {sortedOutputFileShort}")
            logging.info(f"Saved sorted data to {sortedOutputFileShort}")
        except Exception as e:
            print(f"Error saving sorted data to CSV: {e}")
            logging.error(f"Error saving sorted data to CSV {sortedOutputFileShort}: {e}")
    return targetResultsLongDf, targetResultsShortDf


    '''sorted_output_file = os.path.join(outputDirectory, f'close_prices_sorted_long_{selectedDate}.csv')
    sorted_output_file_short = os.path.join(outputDirectory, f'close_prices_sorted_short_{selectedDate}.csv')

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
            logging.error(f"Error saving sorted data to CSV {sorted_output_file_short}: {e}")'''

# =============================================================================
# Trade Type Determination
# =============================================================================

def DetermineTradeType():
    currentTime = datetime.now().time()
    # Example: before 18:00:00 (6 PM) is BUY for long, after is SELL.
    elevenAM = datetime.strptime("18:00:00", "%H:%M:%S").time()
    if currentTime < elevenAM:
        tradeTypeLong = 'BUY'
        tradeTypeShort = 'SELL'
    else:
        tradeTypeLong = 'SELL'
        tradeTypeShort = 'BUY'
    logging.info(f"Determined trade type for long positions: {tradeTypeLong} based on current time: {currentTime}")
    print(f"Determined trade type for long positions: {tradeTypeLong} based on current time: {currentTime}")
    logging.info(f"Determined trade type for short positions: {tradeTypeShort} based on current time: {currentTime}")
    print(f"Determined trade type for short positions: {tradeTypeShort} based on current time: {currentTime}")
    return tradeTypeLong, tradeTypeShort

def GetSymbolsFromPostgresForDate(dateStr):
    queryDate = datetime.strptime(dateStr, "%Y-%m-%d")
    dateForSql = queryDate.strftime("%Y-%m-%d")
    sqlQuery = f""" SELECT DISTINCT ticker_name_inclusion
                FROM NiftyHistoricalIndex
                WHERE event_date_inclusion <= '{dateForSql}'
                AND (event_date_exclusion IS NULL OR event_date_exclusion >= '{dateForSql}');
                """
    connection = None
    symbolsList = []
    try:
        connection = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="1812",
            host="localhost",
            port=5432
        )
        with connection.cursor() as cur:
            cur.execute(sqlQuery)
            rows = cur.fetchall()
            symbolsList = [r[0] for r in rows if r[0] is not None]
    except Exception as e:
        logging.error(f"Error fetching symbols from Postgres: {e}")
    finally:
        if connection:
            connection.close()
    return symbolsList

# =============================================================================
# Main Routine – Supporting multiple-year periods with yearly symbol list updates.
# Also ensures that no date after today is selected.
# =============================================================================

def Main():
    # Hardcoded for example – in practice these may be user inputs
    print("\nEnter the start date (YYYY-MM-DD):")
    startDateInput = '1999-01-20'
    print("\nEnter the end date (YYYY-MM-DD):")
    endDateInput = '2025-02-25'
    try:
        startDateObj = datetime.strptime(startDateInput, "%Y-%m-%d")
        endDateObj = datetime.strptime(endDateInput, "%Y-%m-%d")
        if endDateObj < startDateObj:
            raise ValueError("End date must be after start date.")
    except ValueError as e:
        print(f"Error: {e}")
        logging.error(f"Incorrect date range: {e}")
        return

    allTargetResultsLong = []
    allTargetResultsShort = []

    # Process each year in the range
    for year in range(startDateObj.year, endDateObj.year + 1):
        print(f"\nProcessing year: {year}")
        # For this year, determine the effective start and end dates
        effectiveStart = max(datetime(year, 1, 1), startDateObj)
        effectiveEnd = min(datetime(year, 12, 31), endDateObj)
        # Create a business day date range for the year’s portion
        dateRange = pd.bdate_range(start=effectiveStart, end=effectiveEnd).strftime("%Y-%m-%d").tolist()
        #print(f"Processing dates: {dateRange}")

        # Update the constituent list for this year
        if year < 2021:
            symbols = GetSymbolsFromPostgresForDate(effectiveStart.strftime("%Y-%m-%d"))
        else:
            dfSymbols = ReadCsvFile(Nifty500ConstituentList, delimiter=',')
            if 'Symbol' in dfSymbols.columns:
                symbols = dfSymbols['Symbol'].dropna().unique().tolist()
            else:
                print("Error: 'Symbol' column not found in CSV file.")
                logging.error("Symbol column not found in CSV.")
                exit(1)
        print(f"Total symbols for {year}: {len(symbols)}")
        print(symbols)
        # Set technical parameters
        try:
            lookbackPeriod = int('120')
            smaWindow = int('20')
            stdDevWindow = int('21')
        except ValueError as e:
            print(f"Error in input parameters: {e}")
            logging.error(f"Input parameter error: {e}")
            exit(1)
        requiredPeriod = max(smaWindow, stdDevWindow)
        if lookbackPeriod < requiredPeriod:
            print(f"Adjusting lookback period from {lookbackPeriod} to {requiredPeriod}")
            logging.info(f"Adjusted lookback period from {lookbackPeriod} to {requiredPeriod}")
            lookbackPeriod = requiredPeriod

        # Extend trading days so forward lookup works (add at least 2 extra days)
        extendedEnd = effectiveEnd + timedelta(days=2)
        tradingDays = pd.bdate_range(end=extendedEnd, periods=lookbackPeriod + len(dateRange)).strftime("%Y-%m-%d").tolist()
        print(f"Fetching data for {year} from {tradingDays[0]} to {tradingDays[-1]}")

        dfClose = FetchLtp(symbols, tradingDays, smaWindow, stdDevWindow, batchSize=len(symbols), pause=1)
        # Save raw data for this year (optional)
        outputDirectoryRaw = IntraDayDirectory
        outputFileRaw = os.path.join(outputDirectoryRaw, f"close_prices_individual_{year}.csv")
        SaveToCsv(dfClose, outputFileRaw, None)

        # Now, for each business day in the effective period, process sorted data and accumulate target results
        outputDirectory = IntraDayDirectoryHistory
        targetResultsLongDf = pd.DataFrame(columns=['date', 'returns'])
        targetResultsShortDf = pd.DataFrame(columns=['date', 'returns'])
        for selectedDateInput in dateRange:
            print(f"Processing data for date: {selectedDateInput}")
            outputFile = os.path.join(outputDirectory, f"close_prices_individual_{selectedDateInput}.csv")
            dfCloseDate = dfClose[dfClose['Date'] == selectedDateInput]
            SaveToCsv(dfCloseDate, outputFile, selectedDateInput)
            targetResultsLongDf, targetResultsShortDf = SaveSortedToCsv(dfClose, selectedDateInput, outputDirectory, targetResultsLongDf, targetResultsShortDf)
            #print(targetResultsLongDf, targetResultsShortDf)

        # Save annual target results
        targetResultsLongFile = os.path.join(outputDirectory, f'target_results_long_{year}.csv')
        targetResultsShortFile = os.path.join(outputDirectory, f'target_results_short_{year}.csv')
        targetResultsLongDf['pnl'] = (((targetResultsLongDf['returns'] - CommissionPercent) * CapitalRiskedPerLongTrade * NumberOfStocksToSelectLowestOpenPrice) / 100) - FixedCommissionCost
        targetResultsShortDf['pnl'] = (((targetResultsShortDf['returns'] + CommissionPercent) * CapitalRiskedPerShortTrade * -1 * NumberOfStocksToSelectHighestOpenPrice) / 100) - FixedCommissionCost

        try:
            targetResultsLongDf.to_csv(targetResultsLongFile, index=False)
            print(f"Saved accumulated target results for long positions for {year} to {targetResultsLongFile}")
            logging.info(f"Saved accumulated target results for long positions for {year} to {targetResultsLongFile}")
        except Exception as e:
            print(f"Error saving accumulated target results for long positions for {year}: {e}")
            logging.error(f"Error saving accumulated target results for long positions for {year}: {e}")

        try:
            targetResultsShortDf.to_csv(targetResultsShortFile, index=False)
            print(f"Saved accumulated target results for short positions for {year} to {targetResultsShortFile}")
            logging.info(f"Saved accumulated target results for short positions for {year} to {targetResultsShortFile}")
        except Exception as e:
            print(f"Error saving accumulated target results for short positions for {year}: {e}")
            logging.error(f"Error saving accumulated target results for short positions for {year}: {e}")

        allTargetResultsLong.append(targetResultsLongDf)
        allTargetResultsShort.append(targetResultsShortDf)

    combinedLong = pd.concat(allTargetResultsLong, ignore_index=True)
    combinedShort = pd.concat(allTargetResultsShort, ignore_index=True)
    combinedLongFile = os.path.join(outputDirectory, f'target_results_long_{startDateInput}_to_{endDateInput}.csv')
    combinedShortFile = os.path.join(outputDirectory, f'target_results_short_{startDateInput}_to_{endDateInput}.csv')
    try:
        combinedLong.to_csv(combinedLongFile, index=False)
        print(f"Saved combined target results for long positions to {combinedLongFile}")
        logging.info(f"Saved combined target results for long positions to {combinedLongFile}")
    except Exception as e:
        print(f"Error saving combined target results for long positions: {e}")
        logging.error(f"Error saving combined target results for long positions: {e}")
    try:
        combinedShort.to_csv(combinedShortFile, index=False)
        print(f"Saved combined target results for short positions to {combinedShortFile}")
        logging.info(f"Saved combined target results for short positions to {combinedShortFile}")
    except Exception as e:
        print(f"Error saving combined target results for short positions: {e}")
        logging.error(f"Error saving combined target results for short positions: {e}")

    print("\nProcessing complete for all years.")

if __name__ == "__main__":
    Main()
