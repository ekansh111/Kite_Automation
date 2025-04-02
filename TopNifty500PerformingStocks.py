import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from Directories import *  # Assuming this has paths like `Nifty500ConstituentFilePath`, etc.
from datetime import datetime, date, timedelta
import os

def processNifty500Momentum(startDate, endDate):
    """
    Reads the Nifty 500 constituent file, fetches daily close prices for each symbol from `startDate` to `endDate`,
    computes absolute momentum returns, filters out the top 50 tickers, calculates positive/negative day stats,
    computes 'fip' and 'sgn_fip_product', and saves two CSV files:
      1) 'AbsoluteMomentum_{endDate}.csv' for all tickers.
      2) 'RelativeMomentum_{endDate}.csv' for the top 50.

    Returns:
    --------
    top10PercentDf : DataFrame
        The top 10% DataFrame (top 50 for Nifty 500), with additional stats, including 'Start Date' and 'End Date'.
    """
    print("Caution!!!  Make sure that the Nifty 500 constituent file is updated monthly")
    print("Caution!!!  Ensure that Nifty 500 is above the 200MA")

    # Step 1: Read Ticker Names
    filePath = Nifty500ConstituentFilePath
    dfSymbols = pd.read_csv(filePath)
    tickers = dfSymbols['Symbol'].tolist()

    # Append '.NS' suffix for NSE tickers
    tickers = [ticker + '.NS' for ticker in tickers if isinstance(ticker, str)]
    tickers = list(set(filter(None, tickers)))  # remove duplicates/empty

    print(f"Fetching data from {startDate} to {endDate} for {len(tickers)} tickers...")

    # Download daily Close prices
    data = yf.download(tickers, start=startDate, end=endDate)['Close']
    print(data)

    # Step 3 & 4: Compute absolute momentum
    tickerList = []
    startPriceList = []
    endPriceList = []
    returnList = []

    for ticker in tickers:
        if ticker in data.columns:
            series = data[ticker].dropna()
            if not series.empty:
                firstClose = series.iloc[0]
                lastClose = series.iloc[-1]
                absReturn = ((lastClose - firstClose) / firstClose) * 100

                # Remove '.NS' suffix for final output
                cleanSymbol = ticker.replace('.NS', '')

                tickerList.append(cleanSymbol)
                startPriceList.append(firstClose)
                endPriceList.append(lastClose)
                returnList.append(absReturn)

    resultsDf = pd.DataFrame({
        'Symbol': tickerList,
        'Start Price': startPriceList,
        'End Price': endPriceList,
        'Return (%)': returnList
    })

    # Sign of returns
    resultsDf['sgn(return)'] = resultsDf['Return (%)'].apply(
        lambda x: 1 if x > 0 else (0 if x == 0 else -1)
    )

    # Rank by performance (descending by Return (%))
    resultsDf.sort_values(by='Return (%)', ascending=False, inplace=True)
    resultsDf['Rank'] = range(1, len(resultsDf) + 1)
    resultsDf.reset_index(drop=True, inplace=True)

    # Build a filename with endDate
    endDateStr = endDate.strftime("%Y-%m-%d")

    # 1) Save "Absolute Momentum Returns" for all tickers
    absoluteOutputFile = os.path.join(
        MomentumFilesOutputDirectory,
        f"AbsoluteMomentum_{endDateStr}.csv"
    )
    resultsDf.to_csv(absoluteOutputFile, index=False)
    print(f"Absolute momentum returns saved to: {absoluteOutputFile}")

    # ----------------------------
    # Step 5: Focus on top 50
    # ----------------------------
    top10PercentDf = resultsDf.head(50).copy()

    positiveDaysList = []
    negativeDaysList = []
    percentPositiveDaysList = []
    percentNegativeDaysList = []
    fipList = []

    for symbol in top10PercentDf['Symbol']:
        ticker = symbol + '.NS'
        if ticker in data.columns:
            closePrices = data[ticker].dropna()
            dailyReturns = closePrices.pct_change().dropna()
            numPositiveDays = (dailyReturns > 0).sum()
            numNegativeDays = (dailyReturns < 0).sum()
            totalDays = len(dailyReturns)

            if totalDays == 0:
                posDaysPct = None
                negDaysPct = None
                fipVal = None
            else:
                posDaysPct = (numPositiveDays / totalDays) * 100
                negDaysPct = (numNegativeDays / totalDays) * 100
                fipVal = negDaysPct - posDaysPct

            positiveDaysList.append(numPositiveDays)
            negativeDaysList.append(numNegativeDays)
            percentPositiveDaysList.append(posDaysPct)
            percentNegativeDaysList.append(negDaysPct)
            fipList.append(fipVal)
        else:
            positiveDaysList.append(None)
            negativeDaysList.append(None)
            percentPositiveDaysList.append(None)
            percentNegativeDaysList.append(None)
            fipList.append(None)

    top10PercentDf['Positive Days'] = positiveDaysList
    top10PercentDf['Negative Days'] = negativeDaysList
    top10PercentDf['% Positive Days'] = percentPositiveDaysList
    top10PercentDf['% Negative Days'] = percentNegativeDaysList
    top10PercentDf['fip'] = fipList

    # Ensure 'sgn(return)' is present
    if 'sgn(return)' not in top10PercentDf.columns:
        top10PercentDf['sgn(return)'] = top10PercentDf['Return (%)'].apply(
            lambda x: 1 if x > 0 else (0 if x == 0 else -1)
        )

    # sgn_fip_product
    top10PercentDf['sgn_fip_product'] = top10PercentDf['sgn(return)'] * top10PercentDf['fip']

    # === NEW MULTI-COLUMN SORT LOGIC ===
    # Sort primarily by 'fip' ascending, and if there's a tie in fip, 
    # sort by 'Return (%)' descending
    top10PercentDf.sort_values(
        by=['fip', 'Return (%)'],
        ascending=[True, False],
        inplace=True
    )
    top10PercentDf.reset_index(drop=True, inplace=True)

    # Add Start/End Date columns
    top10PercentDf['Start Date'] = startDate.strftime("%Y-%m-%d")
    # We'll treat endDate - 1 day as the "final" day if that matches your logic
    top10PercentDf['End Date'] = (endDate - timedelta(days=1)).strftime("%Y-%m-%d")

    print(top10PercentDf)

    # 2) Save "Relative Momentum Returns" (top 50)
    relativeOutputFile = os.path.join(
        MomentumFilesOutputDirectory,
        f"RelativeMomentum_{endDateStr}.csv"
    )
    top10PercentDf.to_csv(relativeOutputFile, index=False)
    print(f"Relative momentum returns (top 50) saved to: {relativeOutputFile}")

    return top10PercentDf


def checkNifty500MonthlySmaAbove(periodYears=5, smaMonths=9):
    """
    Downloads daily data for ^CRSLDX up to the last day of the previous month,
    resamples to monthly by selecting the last trading day of each month,
    computes an SMA over `smaMonths` months, and returns True if the latest
    monthly close (previous month) is above that SMA, else False.

    Parameters:
    -----------
    periodYears : int
        How many years of data to fetch. Default = 5.
    smaMonths : int
        The window length for the monthly SMA. Default = 9.

    Returns:
    --------
    bool
        True if the final monthly close > monthly SMA, else False.
    """

    # 1. Determine the last day of the previous month.
    #    If today is 2025-03-16, 'thisMonth1' = 2025-03-01; subtract 1 day => 2025-02-28.
    today = date.today()
    thisMonth1 = date(today.year, today.month, 1)
    lastDayPrevMonth = thisMonth1 - timedelta(days=1)

    # 2. Compute the start date ~periodYears years back
    startDate = lastDayPrevMonth.replace(year=lastDayPrevMonth.year - periodYears)

    print(f"Fetching daily data from {startDate} to {lastDayPrevMonth +  timedelta(days=1)} (up to previous month's last day)")

    # 3. Download daily data with a specific start and end
    df_daily = yf.download("^CRSLDX", start=startDate, end=(lastDayPrevMonth + timedelta(days=1)), interval="1d")
    if df_daily.empty:
        print("No daily data returned from Yahoo for ^CRSLDX.")
        return False

    # 4. If you have a multi-level column index, flatten it:
    if isinstance(df_daily.columns, pd.MultiIndex):
        # Drop the top level or second level depending on how your columns appear
        df_daily.columns = df_daily.columns.droplevel(1) 
        # Alternative approach: df_daily.columns = df_daily.columns.get_level_values(-1)
    #print(df_daily)
    # 5. Resample daily to monthly (last trading day each month)
    df_monthly = df_daily.resample("M").last()  # 'M' => month-end frequency
    #print(df_monthly)
    if df_monthly.empty:
        print("No monthly data available after resampling.")
        return False

    # 6. Drop NaNs from monthly Close
    if "Close" not in df_monthly.columns:
        print("No 'Close' column found in df_monthly!")
        print("Columns found:", df_monthly.columns)
        return False

    df_monthly = df_monthly.dropna(subset=["Close"])
    if df_monthly.empty:
        print("No valid monthly Close data found for ^CRSLDX.")
        return False

    # 7. Compute the SMA for 'smaMonths' rows
    df_monthly["Sma"] = df_monthly["Close"].rolling(window=smaMonths).mean()

    print("\n===== Monthly Data (last few rows) =====")
    print(df_monthly.tail(10))

    # 8. Grab the last row's monthly close and SMA
    lastClose = df_monthly["Close"].iloc[-1]
    lastSma   = df_monthly["Sma"].iloc[-1]

    # 9. Check if we had enough data for a full SMA
    if pd.isna(lastSma):
        print(f"\nInsufficient monthly bars to compute a {smaMonths}-month SMA.")
        return False

    print(f"\nLatest monthly close (end of prev month): {lastClose}, "
          f"{smaMonths}-month SMA: {lastSma}")
    return lastClose > lastSma

def compareMomentumStocksHoldings(numPortfolioStocks, investmentPerShare=80000):
    """
    1. Figures out the current month's start/end for a 1-year window and the previous month's 1-year window.
    2. Calls processNifty500Momentum(...) for each period.
    3. Compares the top tickers from each run, suggests which to ADD and which to REMOVE
       from the final portfolio of size 'numPortfolioStocks'.
    4. Adds a new 'Price' column containing the *current* close price from yfinance,
       and a new 'Quantity' column for how many shares to buy, based on 'investmentPerShare / Price',
       for symbols that need to be added only.
    5. Saves the suggestions in a CSV file for both changes and final portfolio.

    Parameters
    ----------
    numPortfolioStocks : int
        Number of final stocks to hold in the portfolio.
    investmentPerShare : float
        Amount of capital allocated per share you are ADDING.
        Default = 80000.
    """

    today = date.today()

    # 1) Current iteration: from 1 year ago to last day of previous month
    thisMonthStart = date(today.year, today.month, 1)
    endDateCurrent = thisMonthStart - timedelta(days=1)
    startDateCurrent = endDateCurrent - timedelta(days=365)

    print(f"\n--- CURRENT iteration: {startDateCurrent} to {endDateCurrent} ---")
    top10PercentCurrentMonthDf = processNifty500Momentum(
        startDateCurrent,
        endDateCurrent + timedelta(days=1)  # yfinance 'end' is exclusive
    )

    # 2) Previous iteration: from 1 year ago to last day of the month prior
    import pandas as pd
    previousMonthStart = thisMonthStart - pd.DateOffset(months=1)
    lastDayPrevMonth = previousMonthStart - pd.DateOffset(days=1)
    endDatePrevious = lastDayPrevMonth.date()
    startDatePrevious = endDatePrevious - timedelta(days=365)

    print(f"\n--- PREVIOUS iteration: {startDatePrevious} to {endDatePrevious} ---")
    '''top10PercentPreviousMonthDf = processNifty500Momentum(
        startDatePrevious,
        endDatePrevious + timedelta(days=1)
    )'''
    top10PercentPreviousMonthDf = fetchPreviousMonthMomentumDf()

    # 3) Sort each DF by 'fip' ascending, pick top N
    top10PercentCurrentMonthDf.sort_values(by='fip', ascending=True, inplace=True)
    top10PercentPreviousMonthDf.sort_values(by='fip', ascending=True, inplace=True)

    finalCurrentSymbols = top10PercentCurrentMonthDf.head(numPortfolioStocks)['Symbol'].tolist()
    finalPreviousSymbols = top10PercentPreviousMonthDf.head(numPortfolioStocks)['Symbol'].tolist()

    setCurrent = set(finalCurrentSymbols)
    setPrevious = set(finalPreviousSymbols)

    # 4) Add/Remove calculation
    toAdd = setCurrent - setPrevious
    toRemove = setPrevious - setCurrent

    print(f"\nNumber of stocks to hold in final portfolio: {numPortfolioStocks}")
    print(f"Symbols in CURRENT portfolio = {setCurrent}")
    print(f"Symbols in PREVIOUS portfolio = {setPrevious}")
    print(f"To ADD: {toAdd}")
    print(f"To REMOVE: {toRemove}")

    # 5) Build changes DataFrame
    changes = []
    for sym in sorted(toAdd):
        changes.append({"Symbol": sym, "Action": "ADD"})
    for sym in sorted(toRemove):
        changes.append({"Symbol": sym, "Action": "REMOVE"})
    changesDf = pd.DataFrame(changes)

    # Build the final portfolio DataFrame from the current set
    finalPortfolioDf = pd.DataFrame({"Symbol": sorted(setCurrent)})

    # 6) Retrieve the *current* close price
    # We'll add ".NS" for yfinance
    allSymbols = setCurrent | setPrevious  # union of both sets
    tickerList = [sym + ".NS" for sym in sorted(allSymbols)]

    # Download just today's data (if available). `period="1d"` => last trading day's data
    df_price = yf.download(
        tickers=tickerList,
        period="1d",
        interval="1d"
    )["Close"]  # Just the close

    # 7) Build a map: 'Symbol' => current close price
    priceMap = {}
    if isinstance(df_price, pd.DataFrame):
        # multiple tickers
        for c in df_price.columns:
            symbolNoSuffix = c.replace(".NS", "")
            val = df_price[c].dropna()
            priceMap[symbolNoSuffix] = val.iloc[-1] if not val.empty else None
    else:
        # single ticker => a Series
        singleSym = df_price.name.replace(".NS", "")
        val = df_price.dropna()
        priceMap[singleSym] = val.iloc[-1] if not val.empty else None

    # 8) Add 'Price' column to changesDf and finalPortfolioDf
    changesDf['Price'] = changesDf['Symbol'].map(priceMap)
    finalPortfolioDf['Price'] = finalPortfolioDf['Symbol'].map(priceMap)

    # 9) Add "Quantity" column to both DataFrames:
    # For changesDf: Only "ADD" rows get quantity, "REMOVE" get None
    changesDf['Quantity'] = None
    maskChangesAdd = changesDf['Action'] == 'ADD'

    # If Price is None or 0, remain None
    changesDf.loc[maskChangesAdd, 'Quantity'] = (
        investmentPerShare / changesDf.loc[maskChangesAdd, 'Price']
    ).where(changesDf.loc[maskChangesAdd, 'Price'] != 0).round(0)

    # For finalPortfolioDf: Only newly added symbols get quantity, others remain None
    finalPortfolioDf['Quantity'] = None
    maskAddInPortfolio = finalPortfolioDf['Symbol'].isin(toAdd)
    finalPortfolioDf.loc[maskAddInPortfolio, 'Quantity'] = (
        investmentPerShare / finalPortfolioDf.loc[maskAddInPortfolio, 'Price']
    ).where(finalPortfolioDf.loc[maskAddInPortfolio, 'Price'] != 0).round(0)

    # 10) Save results
    endDateCurrentStr = endDateCurrent.strftime("%Y-%m-%d")

    changesFilePath = os.path.join(
        MomentumFilesOutputDirectory,
        f"PortfolioChanges_{endDateCurrentStr}.csv"
    )
    portfolioFilePath = os.path.join(
        MomentumFilesOutputDirectory,
        f"Portfolio_{endDateCurrentStr}.csv"
    )

    changesDf.to_csv(changesFilePath, index=False)
    finalPortfolioDf.to_csv(portfolioFilePath, index=False)

    print(f"\nChanges saved to: {changesFilePath}")
    print(f"Final Portfolio saved to: {portfolioFilePath}")

    return changesDf, finalPortfolioDf

def fetchPreviousMonthMomentumDf():
    """
    Fetches the top 10% relative momentum DataFrame from the previous month's directory.

    Returns:
    --------
    pd.DataFrame
        DataFrame containing relative momentum data from the previous month.
    """

    today = date.today()
    firstDayCurrentMonth = today.replace(day=1)
    lastDayPrevMonth = firstDayCurrentMonth - pd.Timedelta(days=1)

    monthStr = lastDayPrevMonth.strftime("%b").upper()
    yearStr = lastDayPrevMonth.strftime("%Y")

    prevMonthDir = f"{monthStr}{yearStr}"

    # Corrected file path with fixed '01' date
    relativeMomentumFileName = f"RelativeMomentum_{lastDayPrevMonth.strftime('%Y-%m')}-01.csv"
    relativeMomentumFilePath = rf"C:\Users\ekans\OneDrive\Documents\Trading\Momentum Stock Investing Data\{prevMonthDir}\{relativeMomentumFileName}"

    if not os.path.exists(relativeMomentumFilePath):
        raise FileNotFoundError(f"File not found: {relativeMomentumFilePath}")

    # Read the CSV file
    top10PercentPreviousMonthDf = pd.read_csv(relativeMomentumFilePath)

    print(f"Loaded previous month's data from {relativeMomentumFilePath}")

    return top10PercentPreviousMonthDf


if __name__ == "__main__":

    #if(checkNifty500MonthlySmaAbove()):
        compareMomentumStocksHoldings(numPortfolioStocks=20)
    #else:
    #    print("Nifty 500 is below its 9 Month SMA vslue, No trades to be taken, Exit ALL positions")

