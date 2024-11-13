"""
This script implements a pairs trading strategy based on mean reversion and cointegration analysis.

**Main Functionalities:**

1. **Data Preparation:**
   - Downloads historical stock data for specified sectors from Yahoo Finance using `yfinance`.
   - Organizes the data by sectors and filters valid tickers.
   - Calculates daily returns for the cointegration period.

2. **Cointegration Testing:**
   - Performs cointegration tests on pairs within each sector over a specified cointegration period.
   - Identifies pairs with p-values below a threshold (e.g., 0.05) indicating cointegration.
   - Visualizes cointegration p-values using heatmaps and saves them.

3. **Ordinary Least Squares (OLS) Regression:**
   - Performs OLS regression on cointegrated pairs to calculate hedge ratios.
   - Conducts statistical tests (t-test, F-test, Durbin-Watson, Jarque-Bera, ADF test) on residuals.
   - Plots and saves spread and residuals charts for each pair.

4. **Signal Generation:**
   - Generates trading signals based on Z-scores of the price ratios between pairs.
   - Defines entry and exit signals based on upper and lower Z-score thresholds.

5. **Backtesting:**
   - Backtests the trading strategy over a training period to evaluate performance.
   - Selects top-performing pairs based on training period profits.
   - Backtests the selected pairs over a testing period using updated signals.

6. **Risk Management:**
   - Implements drawdown monitoring to halt trading if maximum drawdown exceeds 25%.
   - Resumes trading when Z-score returns to normal range (-1 to 1).

7. **Performance Evaluation and Reporting:**
   - Calculates profits, returns, and maximum drawdowns for each pair and sector.
   - Aggregates results and plots cumulative profits per sector.
   - Saves results, charts, and ongoing positions to specified directories.

**Usage:**

- Update the `sectors` dictionary with the desired sectors and their respective stock tickers.
- Adjust the date ranges for total data, training, and testing periods.
- Configure the parameters such as `windowsize`, `top_pairs_to_select`, and `num_pairs_to_select`.
- Run the script to perform cointegration analysis, backtesting, and generate reports.
- Ensure that the required directories exist or adjust paths accordingly.

**Dependencies:**

- Python 3.x
- Libraries: `pandas`, `numpy`, `yfinance`, `statsmodels`, `matplotlib`, `seaborn`, `datetime`, `os`, `re`, `itertools`
- Make sure to install these libraries using `pip` if they are not already installed.

**Notes:**

- The script uses the 'Agg' backend for matplotlib to allow non-interactive plotting (useful when running on a server without display).
- The script includes an option to perform or skip statistical tests in the OLS regression (`perform_statistical_tests` variable).
- Risk management is implemented by monitoring drawdowns and halting trading if necessary.
- Ongoing positions at the end of the backtest are saved for further analysis.
- All plots and data are saved in specified directories for review.

"""


import matplotlib
matplotlib.use('Agg')  # Use the 'Agg' backend for non-interactive plotting

import pandas as pd
import numpy as np
import yfinance as yf
import statsmodels.api as sm
import statsmodels.tsa.stattools as ts
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import os
import re  # Import re for sanitizing filenames
from itertools import combinations
from statsmodels.stats.stattools import durbin_watson


# Define the sanitize_filename function
def sanitize_filename(s):
    # Replace spaces with underscores
    s = s.replace(' ', '_')
    # Replace ampersand with 'and'
    s = s.replace('&', 'and')
    # Remove invalid characters
    s = re.sub(r'[<>:"/\\|?*]', '', s)
    return s

# Define the sectors and their top ten stocks
sectors = {
    "Software & Services": [
        "HCLTECH.NS",
        "INFY.NS",
        "BSOFT.NS",
        "MPHASIS.NS",
        "NAUKRI.NS",
        "COFORGE.NS",
        "OFSS.NS",
        "PERSISTENT.NS",
        "TCS.NS",
        "TECHM.NS",
        "WIPRO.NS",
        "LTIM.NS",
        "LTTS.NS",
        "INDIAMART.NS"
    ],
    "Chemicals & Petrochemicals": [
        "AARTIIND.NS",
        "ATUL.NS",
        "DEEPAKNTR.NS",
        "NAVINFLUOR.NS",
        "PIDILITIND.NS",
        "PIIND.NS",
        "SRF.NS",
        "TATACHEM.NS",
        "UPL.NS"
    ],
    "Media": [
        "PVRINOX.NS",
        "SUNTV.NS"
    ],
    "Food Beverages & Tobacco": [
        "BALRAMCHIN.NS",
        "ITC.NS",
        "UNITDSPR.NS",
        "TATACONSUM.NS",
        "UBL.NS"
    ],
    "Retailing": [
        "BATAINDIA.NS",
        "ABFRL.NS",
        "TRENT.NS"
    ],
    "FMCG": [
        "BRITANNIA.NS",
        "COLPAL.NS",
        "DABUR.NS",
        "GODREJCP.NS",
        "HINDUNILVR.NS",
        "MARICO.NS",
        "NESTLEIND.NS"
    ],
    "Textiles Apparels & Accessories": [
        "PAGEIND.NS",
        "TITAN.NS"
    ],
    "Transportation": [
        "ADANIPORTS.NS",
        "INDIGO.NS"
    ],
    "Telecom Services": [
        "BHARTIARTL.NS",
        "IDEA.NS",
        "INDUSTOWER.NS",
        "TATACOMM.NS"
    ],
    "Diversified": [
        "BAJAJFINSV.NS",
        "LTF.NS",
        "ABCAPITAL.NS"
    ],
    "Oil & Gas": [
        "BPCL.NS",
        "HINDPETRO.NS",
        "IOC.NS",
        "ONGC.NS",
        "PETRONET.NS",
        "RELIANCE.NS"
    ],
    "Metals & Mining": [
        "COALINDIA.NS",
        "HINDALCO.NS",
        "HINDCOPPER.NS",
        "JINDALSTEL.NS",
        "JSWSTEEL.NS",
        "NATIONALUM.NS",
        "NMDC.NS",
        "SAIL.NS",
        "VEDL.NS",
        "TATASTEEL.NS"
    ],
    "Diversified Consumer Services": [
        "APOLLOHOSP.NS",
        "ASIANPAINT.NS",
        "BERGEPAINT.NS",
        "LALPATHLAB.NS",
        "METROPOLIS.NS",
        "IRCTC.NS"
    ],
    "Pharmaceuticals & Biotechnology": [
        "ABBOTINDIA.NS",
        "AUROPHARMA.NS",
        "BIOCON.NS",
        "ZYDUSLIFE.NS",
        "CIPLA.NS",
        "DIVISLAB.NS",
        "DRREDDY.NS",
        "GLENMARK.NS",
        "GRANULES.NS",
        "IPCALAB.NS",
        "LUPIN.NS",
        "SUNPHARMA.NS",
        "TORNTPHARM.NS",
        "SYNGENE.NS",
        "ALKEM.NS",
        "LAURUSLABS.NS"
    ],
    "Realty": [
        "DLF.NS",
        "GODREJPROP.NS",
        "OBEROIRLTY.NS"
    ],
    "Automobiles & Auto Components": [
        "APOLLOTYRE.NS",
        "ASHOKLEY.NS",
        "BAJAJ-AUTO.NS",
        "BALKRISIND.NS",
        "BOSCHLTD.NS",
        "EICHERMOT.NS",
        "ESCORTS.NS",
        "EXIDEIND.NS",
        "HEROMOTOCO.NS",
        "M&M.NS",
        "MARUTI.NS",
        "MOTHERSON.NS",
        "MRF.NS",
        "TATAMOTORS.NS",
        "TVSMOTOR.NS"
    ],
    
    "banking_companies" : [
    "AXISBANK.NS",       # Axis Bank
    "BANKBARODA.NS",     # Bank of Baroda
    "CANBK.NS",          # Canara Bank
    "CUB.NS",            # City Union Bank
    "FEDERALBNK.NS",     # Federal Bank
    "HDFCBANK.NS",       # HDFC Bank
    "ICICIBANK.NS",      # ICICI Bank
    "INDUSINDBK.NS",     # IndusInd Bank
    "KOTAKBANK.NS",      # Kotak Mahindra Bank
    "PNB.NS",            # Punjab National Bank
    "SBIN.NS",           # State Bank of India
    "RBLBANK.NS",        # RBL Bank
    "IDFCFIRSTB.NS",     # IDFC First Bank
    "BANDHANBNK.NS",     # Bandhan Bank
    "AUBANK.NS"          # AU Small Finance Bank
    ],

    "finance_companies" : [
    "BAJFINANCE.NS",     # Bajaj Finance
    "CANFINHOME.NS",     # Can Fin Homes
    "CHOLAFIN.NS",       # Cholamandalam Finance
    "HDFCLIFE.NS",       # HDFC Life Insurance
    "HDFCAMC.NS",        # HDFC Asset Management
    "LICHSGFIN.NS",      # LIC Housing Finance
    "M&MFIN.NS",         # Mahindra & Mahindra Finance
    "MANAPPURAM.NS",     # Manappuram Finance
    "MFSL.NS",           # Max Financial Services
    "MCX.NS",            # Multi Commodity Exchange
    "MUTHOOTFIN.NS",     # Muthoot Finance
    "PEL.NS",            # Piramal Enterprises
    "PFC.NS",            # Power Finance Corporation
    "RECLTD.NS",         # REC Ltd.
    "SHRIRAMFIN.NS",     # Shriram Finance
    "ICICIPRULI.NS",     # ICICI Prudential Life Insurance
    "SBILIFE.NS",        # SBI Life Insurance
    "ICICIGI.NS",        # ICICI Lombard General Insurance
    "IEX.NS",            # Indian Energy Exchange
    "SBICARD.NS"         # SBI Cards
    ],
    "Consumer Durables": [
        "HAVELLS.NS",
        "VOLTAS.NS",
        "CROMPTON.NS",
        "DIXON.NS",
        "POLYCAB.NS"
    ],
    "General Industrials": [
        "ABB.NS",
        "ASTRAL.NS",
        "BEL.NS",
        "BHARATFORG.NS",
        "BHEL.NS",
        "CUMMINSIND.NS",
        "SIEMENS.NS",
        "HAL.NS"
    ],
    "Commercial Services & Supplies": [
        "ADANIENT.NS",
        "CONCOR.NS"
    ],
    "Cement and Construction": [
        "ACC.NS",
        "AMBUJACEM.NS",
        "GMRINFRA.NS",
        "GRASIM.NS",
        "JKCEMENT.NS",
        "LT.NS",
        "RAMCOCEM.NS",
        "SHREECEM.NS",
        "ULTRACEMCO.NS",
        "DALBHARAT.NS"
    ],
    "Fertilizers": [
        "CHAMBLFERT.NS",
        "COROMANDEL.NS",
        "GNFC.NS"
    ],
    "Utilities": [
        "GAIL.NS",
        "GUJGASLTD.NS",
        "IGL.NS",
        "NTPC.NS",
        "POWERGRID.NS",
        "TATAPOWER.NS",
        "MGL.NS"
    ]
}


TotalCointigratedPairs = {}

# Total date range for data downloading
total_start_date = '2005-01-04'
total_end_date = '2021-12-31'

# Training and testing dates
training_start = '2015-01-02'#'2017-01-02'
training_end = '2021-12-31'#'2022-10-04'
testing_start = '2015-01-02'#'2023-01-02'
testing_end = '2021-12-31'#'2024-10-04'

# Cointegration test date range
cointegration_start_date = '2015-01-02'#'2017-01-02'
cointegration_end_date = '2021-12-31'#'2022-10-04'

# Option to perform or skip statistical tests in OLS regression
perform_statistical_tests = True  # Set to False to skip the statistical tests

windowsize = 252
top_pairs_to_select = 0

# Configurable value for the number of pairs to select based on training period returns
num_pairs_to_select = 20

# Convert date strings to Timestamps
training_start = pd.to_datetime(training_start)
training_end = pd.to_datetime(training_end)
testing_start = pd.to_datetime(testing_start)
testing_end = pd.to_datetime(testing_end)
cointegration_start_date = pd.to_datetime(cointegration_start_date)
cointegration_end_date = pd.to_datetime(cointegration_end_date)

# Directories to save data and charts
base_dir = r"C:\Users\ekans\OneDrive\Documents\Trading\Scripts\Charts"
closing_prices_dir = os.path.join(base_dir, "Closing Prices")
heatmap_dir = os.path.join(base_dir, "Cointegration Heatmaps")
spread_residuals_dir = os.path.join(base_dir, "Spread and Residuals")
portfolio_value_dir = os.path.join(base_dir, "Portfolio Value")
zscore_data_dir = os.path.join(base_dir, "z score")

# Ensure the directories exist
os.makedirs(base_dir, exist_ok=True)
os.makedirs(closing_prices_dir, exist_ok=True)
os.makedirs(heatmap_dir, exist_ok=True)
os.makedirs(spread_residuals_dir, exist_ok=True)
os.makedirs(portfolio_value_dir, exist_ok=True)
os.makedirs(zscore_data_dir, exist_ok=True)

# Function to download stock data
def download_stock_data(tickers, start_date, end_date):
    stock_data = {}
    valid_tickers = []
    for ticker in tickers:
        try:
            data = yf.download(ticker, start=start_date, end=end_date)
            if data.empty:
                print(f"No data for {ticker}. Skipping.")
                continue
            stock_data[ticker] = data['Close']
            valid_tickers.append(ticker)
        except Exception as e:
            print(f"Error downloading {ticker}: {e}")
            continue
    stock_df = pd.DataFrame(stock_data)
    return stock_df, valid_tickers

# Step 1: Download data and compute returns
sector_data = {}
sector_returns = {}
for sector, tickers in sectors.items():
    print(f"Processing sector: {sector}")
    # Download data for the sector
    data, valid_tickers = download_stock_data(tickers, total_start_date, total_end_date)
    if data.empty or len(valid_tickers) < 2:
        print(f"Not enough valid tickers in sector {sector} after download. Skipping sector.")
        continue
    sector_data[sector] = data
    # Compute returns for the correlation matrix over the cointegration period
    try:
        # Ensure the index is datetime
        data.index = pd.to_datetime(data.index)
        # Check if the cointegration period is within the data index
        if cointegration_start_date not in data.index or cointegration_end_date not in data.index:
            print(f"Cointegration period not fully available for sector {sector}. Skipping sector.")
            continue
        returns = data.loc[cointegration_start_date:cointegration_end_date].pct_change().dropna()
        if returns.empty:
            print(f"No returns data for sector {sector} in cointegration period. Skipping sector.")
            continue
        sector_returns[sector] = returns
    except Exception as e:
        print(f"Error processing returns for sector {sector}: {e}")
        continue

    # Plot closing prices of each stock in the sector during the cointegration period
    plt.figure(figsize=(12, 6))
    for ticker in valid_tickers:
        plt.plot(data.loc[cointegration_start_date:cointegration_end_date].index, data[ticker].loc[cointegration_start_date:cointegration_end_date], label=ticker)
    plt.title(f'Closing Prices of {sector} Sector Stocks (Cointegration Period)')
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.legend(loc='upper left')
    plt.grid(True)

    # Sanitize sector name for filenames
    sanitized_sector = sanitize_filename(sector)

    # Save the plot to the closing prices directory
    closing_prices_filename = os.path.join(closing_prices_dir, f"{sanitized_sector}_closing_prices.png")
    plt.savefig(closing_prices_filename)
    plt.close()
    print(f"Closing prices chart saved to {closing_prices_filename}")

# Step 3: Identify cointegrated pairs within each sector
cointegrated_pairs = {}
for sector, data in sector_data.items():
    print(f"Performing cointegration tests for sector: {sector}")

    # Sanitize sector name for filenames
    sanitized_sector = sanitize_filename(sector)

    tickers = data.columns
    pairs = list(combinations(tickers, 2))
    coint_pvalues = pd.DataFrame(index=tickers, columns=tickers)
    coint_pvalues[:] = np.nan  # Initialize with NaNs
    sector_pvalues = []  # List to collect (stock1, stock2, p_value)

    for pair in pairs:
        stock1, stock2 = pair
        # Perform cointegration test on cointegration data only
        try:
            # Extract data for the pair
            series1 = data[stock1].loc[cointegration_start_date:cointegration_end_date]
            series2 = data[stock2].loc[cointegration_start_date:cointegration_end_date]

            # Combine the series into a DataFrame and drop NaNs
            combined_series = pd.concat([series1, series2], axis=1).dropna()
            combined_series.columns = [stock1, stock2]

            # Check if sufficient data is available after dropping NaNs
            if len(combined_series) < 100:  # You can set your own threshold
                print(f"Not enough data for pair {stock1} & {stock2} after dropping NaNs.")
                continue

            # Perform the cointegration test
            score, p_value, _ = ts.coint(
                combined_series[stock1],
                combined_series[stock2]
            )
            coint_pvalues.loc[stock1, stock2] = p_value
            sector_pvalues.append((stock1, stock2, p_value))
        except Exception as e:
            print(f"Error in cointegration test for pair {stock1} & {stock2}: {e}")
            continue

    # After the loop, sort the sector_pvalues by p_value
    sector_pvalues_sorted = sorted(sector_pvalues, key=lambda x: x[2])  # x[2] is p_value

    # Extract pairs with p-value less than 0.05
    pairs_below_threshold = [pair for pair in sector_pvalues_sorted if pair[2] < 0.05]

    print('Pairs below the threshold')
    print(pairs_below_threshold)

    # Determine the final list of pairs based on the logic
    if len(pairs_below_threshold) >= top_pairs_to_select:
        # If more than top_pairs_to_select pairs have p-value < 0.05, select the top ones
        sector_top_pairs = pairs_below_threshold  # You can limit this to top_pairs_to_select if needed
    else:
        # Select all pairs with p-value < 0.05
        sector_top_pairs = pairs_below_threshold
        # If fewer than top_pairs_to_select pairs, add additional pairs with lowest p-values
        additional_pairs_needed = top_pairs_to_select - len(pairs_below_threshold)
        # Exclude pairs already selected
        selected_pairs_set = set(pairs_below_threshold)
        # Get remaining pairs not already selected
        remaining_pairs = [pair for pair in sector_pvalues_sorted if pair not in selected_pairs_set]
        # Add additional pairs to reach top_pairs_to_select
        sector_top_pairs.extend(remaining_pairs[:additional_pairs_needed])

    TotalCointigratedPairs.update(cointegrated_pairs)
    # Update cointegrated_pairs[sector] with the selected pairs
    cointegrated_pairs[sector] = sector_top_pairs
    print("The cointegrated pair mapping is:")
    print(TotalCointigratedPairs)

    print(f"Top pairs for sector {sector}:")
    for pair in sector_top_pairs:
        print(f"{pair[0]} & {pair[1]} with p-value {pair[2]:.5f}")

    # Visualize the p-values heatmap
    plt.figure(figsize=(10, 8))
    sns.heatmap(coint_pvalues.astype(float), annot=True, cmap='coolwarm')
    plt.title(f"Cointegration P-Values for {sector} Sector")
    plt.grid(True)

    # Save the heatmap to the heatmap directory
    heatmap_filename = os.path.join(heatmap_dir, f"{sanitized_sector}_cointegration_heatmap.png")
    plt.savefig(heatmap_filename)
    plt.close()
    print(f"Cointegration heatmap saved to {heatmap_filename}")

# Step 4: Build OLS regression models with the cointegrated pairs
from statsmodels.stats.stattools import durbin_watson

final_pairs = {}
for sector, pairs in cointegrated_pairs.items():
    print(pairs)
    print(sector)
    print(f"Analyzing OLS regression for sector: {sector}")
    valid_pairs = []
    for stock1, stock2, p_value in pairs:
        # Select the predictor as the stock with higher mean close price in training data
        try:
            mean1 = sector_data[sector][stock1][training_start:training_end].mean()
            mean2 = sector_data[sector][stock2][training_start:training_end].mean()
            if mean1 > mean2:
                predictor = stock1
                target = stock2
            else:
                predictor = stock2
                target = stock1

            X = sm.add_constant(sector_data[sector][predictor][training_start:training_end])
            y = sector_data[sector][target][training_start:training_end]
            model = sm.OLS(y, X).fit()
            print('Model')
            print(model)
            # Extract statistical outputs
            hedge_ratio = model.params.iloc[1]
            print('Hedge Ratio')
            print(hedge_ratio)
            t_pvalue = model.pvalues.iloc[1]
            f_pvalue = model.f_pvalue
            omnibus_pvalue = sm.stats.omni_normtest(model.resid)[1]
            dw_stat = durbin_watson(model.resid)
            jb_pvalue = sm.stats.jarque_bera(model.resid)[1]

            # Check criteria
            print(f"Pair: {predictor} & {target}")
            print(f"t_pvalue: {t_pvalue}")
            print(f"f_pvalue: {f_pvalue}")
            print(f"omnibus_pvalue: {omnibus_pvalue}")
            print(f"dw_stat: {dw_stat}")
            print(f"Jarque-Bera p-value: {jb_pvalue}")

            # Check stationarity of residuals
            adf_result = ts.adfuller(model.resid)
            adf_stat = adf_result[0]
            adf_pvalue = adf_result[1]
            critical_value = adf_result[4]['1%']
            critical_value_5_percent = adf_result[4]['5%']

            print(f"adf_pvalue: {adf_pvalue}")
            print(f"adf_stat: {adf_stat}")
            print(f"Critical Value (1%): {critical_value}\n")
            print(f"Critical Value (5%): {critical_value_5_percent}\n")
            
            # Calculate the spread
            spread = sector_data[sector][predictor][training_start:training_end] - \
                     (hedge_ratio * sector_data[sector][target][training_start:training_end])
            
            # Extract residuals from the OLS model
            residuals = model.resid      

            # Plot spread over time
            plt.figure(figsize=(10, 6))
            plt.plot(spread)
            plt.title(f'Spread Over Time for {predictor} & {target}')
            plt.xlabel('Date')
            plt.ylabel('Spread')
            plt.grid(True)
            
            # Save the spread plot
            spread_filename = os.path.join(spread_residuals_dir, f"{sector}_{predictor}_{target}_spread.png")
            plt.savefig(spread_filename)
            plt.close()
            print(f"Spread chart saved to {spread_filename}")
            
            # Plot residuals over time
            plt.figure(figsize=(10, 6))
            plt.plot(residuals)
            plt.title(f'Residuals Over Time for {predictor} & {target}')
            plt.xlabel('Date')
            plt.ylabel('Residuals')
            plt.grid(True)
            
            # Save the residuals plot
            residuals_filename = os.path.join(spread_residuals_dir, f"{sector}_{predictor}_{target}_residuals.png")
            plt.savefig(residuals_filename)
            plt.close()
            print(f"Residuals chart saved to {residuals_filename}")
            
            # Plot residuals vs spread
            plt.figure(figsize=(10, 6))
            plt.scatter(spread, residuals)
            plt.title(f'Residuals vs Spread for {predictor} & {target}')
            plt.xlabel('Spread')
            plt.ylabel('Residuals')
            plt.grid(True)
            
            # Save the residuals vs spread plot
            residuals_vs_spread_filename = os.path.join(spread_residuals_dir, f"{sector}_{predictor}_{target}_residuals_vs_spread.png")
            plt.savefig(residuals_vs_spread_filename)
            plt.close()
            print(f"Residuals vs Spread chart saved to {residuals_vs_spread_filename}")
            
            # Check for acceptable Durbin-Watson statistic
            acceptable_dw = 0 <= dw_stat <= 3

            # For residuals to be normally distributed, we prefer omnibus_pvalue > 0.05

            if perform_statistical_tests:
                if (t_pvalue < 0.05) and (f_pvalue < 0.05) and acceptable_dw and \
                   (adf_pvalue < 0.01) and (adf_stat < critical_value_5_percent):
                    # Pair passes all tests
                    print('The pair has passed all the tests')
                    valid_pairs.append({
                        'predictor': predictor,
                        'target': target,
                        'hedge_ratio': hedge_ratio,
                        'p_value': p_value,
                        'adf_stat': adf_stat,
                        'adf_pvalue': adf_pvalue,
                        'dw_stat': dw_stat,
                        'omnibus_pvalue': omnibus_pvalue,
                        't_pvalue': t_pvalue,
                        'f_pvalue': f_pvalue,
                    })
                else:
                    print(f"Pair {predictor} & {target} did not pass the statistical tests.\n")
            else:
                # Skip statistical tests, accept the pair
                valid_pairs.append({
                    'predictor': predictor,
                    'target': target,
                    'hedge_ratio': hedge_ratio,
                    'p_value': p_value,
                    'adf_stat': adf_stat,
                    'adf_pvalue': adf_pvalue,
                    'dw_stat': dw_stat,
                    'omnibus_pvalue': omnibus_pvalue,
                    't_pvalue': t_pvalue,
                    'f_pvalue': f_pvalue,
                })
        except Exception as e:
            print(f"Error in OLS regression for pair {stock1} & {stock2}: {e}")
            continue

    final_pairs[sector] = valid_pairs

# Proceed with further steps only if there are valid pairs
if not any(final_pairs.values()):
    print("No valid pairs found in any sector.")
else:

    def generate_signals_and_positions(stock1, stock2, data, window, testing_start=None):
        """
        Generates trading signals and positions for two stocks based on their Z-scores,
        and saves the resulting DataFrame to a CSV file.

        Parameters:
        - stock1 (str): The ticker symbol for the first stock (asset1).
        - stock2 (str): The ticker symbol for the second stock (asset2).
        - data (pd.DataFrame): A DataFrame containing historical price data for stock1 and stock2.
        - window (int): The rolling window size for calculating rolling mean and std.
        - testing_start (str): The start date of the testing period (e.g., '2021-01-01').

        Returns:
        - combined (pd.DataFrame): The DataFrame containing prices, signals, positions, and other metrics.
        """
        
        # Create a combined DataFrame with stock prices
        combined = pd.DataFrame(index=data.index)
        combined[stock1] = data[stock1]
        combined[stock2] = data[stock2]

        # Calculate the ratio between the two stocks
        combined['Ratio'] = combined[stock1] / combined[stock2]

        # Calculate rolling statistics
        combined['Rolling Mean'] = combined['Ratio'].rolling(window=window).mean()
        combined['Rolling Std'] = combined['Ratio'].rolling(window=window).std()

        # Calculate Z-Score
        combined['Z-Score'] = (combined['Ratio'] - combined['Rolling Mean']) / combined['Rolling Std']

        # Define upper and lower bounds for trading signals
        combined['Upper Limit'] = 1
        combined['Lower Limit'] = -1

        # Generate trading signals for asset1 based on Z-score and thresholds
        combined['signals1'] = np.where(
            combined['Z-Score'] > combined['Upper Limit'], -1,  # Short signal
            np.where(combined['Z-Score'] < combined['Lower Limit'], 1, 0)  # Long signal or no action
        )

        # Generate trading signals for asset2 (opposite of asset1)
        combined['signals2'] = -combined['signals1']

        # Generate positions based on signal changes (diff from previous day)
        combined['positions1'] = combined['signals1'].diff()
        combined['positions2'] = combined['signals2'].diff()

        # Handle NaN values resulting from diff
        combined['positions1'] = combined['positions1'].fillna(0)
        combined['positions2'] = combined['positions2'].fillna(0)

        # **Update positions on the first day of the testing period**
        if testing_start is not None:
            # Convert testing_start to Timestamp for index alignment
            testing_start_date = pd.to_datetime(testing_start)

            # Check if the testing_start_date is in the index
            if testing_start_date in combined.index:
                first_test_idx = combined.index.get_loc(testing_start_date)
                # For positions1
                if combined['signals1'].iloc[first_test_idx] in [1, -1]:
                    combined.iloc[first_test_idx, combined.columns.get_loc('positions1')] = combined['signals1'].iloc[first_test_idx]
                else:
                    combined.iloc[first_test_idx, combined.columns.get_loc('positions1')] = 0

                # For positions2
                if combined['signals2'].iloc[first_test_idx] in [1, -1]:
                    combined.iloc[first_test_idx, combined.columns.get_loc('positions2')] = combined['signals2'].iloc[first_test_idx]
                else:
                    combined.iloc[first_test_idx, combined.columns.get_loc('positions2')] = 0
            else:
                print(f"Testing start date {testing_start} is not in the data index.")
        
        # Save the combined DataFrame (which includes Z-score) to the zscore_data_dir
        zscore_filename = os.path.join(zscore_data_dir, f"{stock1}_{stock2}_zscore_data.csv")
        combined.to_csv(zscore_filename)
        print(f"Z-score data for {stock1} & {stock2} saved to {zscore_filename}")

        return combined

    def calculate_max_drawdown(portfolio_values):
        """
        Calculates the maximum drawdown of a portfolio value series.
        Returns the maximum drawdown as a positive percentage.
        """
        cumulative_returns = portfolio_values / portfolio_values.iloc[0]
        running_max = cumulative_returns.cummax()
        drawdown = (cumulative_returns - running_max) / running_max
        max_drawdown = drawdown.min()
        return abs(max_drawdown)

    # Updated backtest function with the maximum drawdown logic
    def backtest(signals, initial_investment):
        positions1 = signals['positions1'].fillna(0).copy()
        positions2 = signals['positions2'].fillna(0).copy()
        stock1 = signals.columns[0]
        stock2 = signals.columns[1]

        # Initialize holdings and cash
        cash = initial_investment
        holdings = {'stock1': 0, 'stock2': 0}
        portfolio_values = []
        holdings_stock1 = []
        holdings_stock2 = []
        units_traded_stock1 = []
        units_traded_stock2 = []

        # Initialize variables for drawdown tracking
        peak_value = initial_investment
        max_drawdown = 0
        trading_halted = False

        for i in range(len(signals)):
            date = signals.index[i]
            price1 = signals.iloc[i][stock1]
            price2 = signals.iloc[i][stock2]
            pos1 = positions1.iloc[i]
            pos2 = positions2.iloc[i]
            z_score = signals['Z-Score'].iloc[i]

            units_traded1 = 0
            units_traded2 = 0

            # Calculate total portfolio value before any trades
            total_value = cash + holdings['stock1'] * price1 + holdings['stock2'] * price2

            # Update peak_value and calculate drawdown
            if total_value > peak_value:
                peak_value = total_value
            drawdown = (peak_value - total_value) / peak_value
            if drawdown > max_drawdown:
                max_drawdown = drawdown

            # If drawdown exceeds 25%, exit positions and halt trading
            if max_drawdown >= 0.25 and not trading_halted:
                # Exit positions
                if holdings['stock1'] != 0:
                    cash += holdings['stock1'] * price1
                    units_traded1 = -holdings['stock1']
                    holdings['stock1'] = 0
                if holdings['stock2'] != 0:
                    cash += holdings['stock2'] * price2
                    units_traded2 = -holdings['stock2']
                    holdings['stock2'] = 0
                trading_halted = True
                print(f"Trading halted on {date.strftime('%Y-%m-%d')} due to max drawdown.")
            else:
                # If trading is halted, check if z-score returns between -1 and 1
                if trading_halted:
                    if -1 <= z_score <= 1:
                        trading_halted = False
                        print(f"Trading resumed on {date.strftime('%Y-%m-%d')} as Z-score returned to normal range.")

                if not trading_halted:
                    # Proceed with trade execution
                    # Execute trades for asset1
                    if pos1 == 1:
                        if holdings['stock1'] < 0:
                            # Currently short, need to buy back to exit short position
                            units_to_buy = -holdings['stock1']
                            cost = units_to_buy * price1
                            holdings['stock1'] += units_to_buy  # This brings holdings to zero
                            cash -= cost
                            units_traded1 = units_to_buy  # Positive units
                        elif holdings['stock1'] == 0:
                            # Currently neutral, enter long position
                            # Buy as many units as possible with half the cash
                            max_units = (cash / 2) // price1
                            holdings['stock1'] += max_units
                            cash -= max_units * price1
                            units_traded1 = max_units  # Positive units
                        # If already long, do nothing

                    elif pos1 == -1:
                        if holdings['stock1'] > 0:
                            # Currently long, need to sell to exit long position
                            cash += holdings['stock1'] * price1
                            units_traded1 = -holdings['stock1']  # Negative units
                            holdings['stock1'] = 0
                        elif holdings['stock1'] == 0:
                            # Currently neutral, enter short position
                            # Short as many units as possible with half the cash (assuming unlimited borrowing)
                            max_units = (cash / 2) // price1
                            holdings['stock1'] -= max_units
                            cash += max_units * price1
                            units_traded1 = -max_units  # Negative units
                        # If already short, do nothing

                    # Execute trades for asset2
                    if pos2 == 1:
                        if holdings['stock2'] < 0:
                            # Currently short, need to buy back to exit short position
                            units_to_buy = -holdings['stock2']
                            cost = units_to_buy * price2
                            holdings['stock2'] += units_to_buy  # This brings holdings to zero
                            cash -= cost
                            units_traded2 = units_to_buy  # Positive units
                        elif holdings['stock2'] == 0:
                            # Currently neutral, enter long position
                            max_units = (cash / 2) // price2
                            holdings['stock2'] += max_units
                            cash -= max_units * price2
                            units_traded2 = max_units  # Positive units
                        # If already long, do nothing

                    elif pos2 == -1:
                        if holdings['stock2'] > 0:
                            # Currently long, need to sell to exit long position
                            cash += holdings['stock2'] * price2
                            units_traded2 = -holdings['stock2']  # Negative units
                            holdings['stock2'] = 0
                        elif holdings['stock2'] == 0:
                            # Currently neutral, enter short position
                            max_units = (cash / 2) // price2
                            holdings['stock2'] -= max_units
                            cash += max_units * price2
                            units_traded2 = -max_units  # Negative units
                        # If already short, do nothing
                    # Else, holdings remain the same
                else:
                    # Trading is halted, no new positions are opened
                    units_traded1 = 0
                    units_traded2 = 0

            # Update total portfolio value after trades (for next iteration)
            total_value = cash + holdings['stock1'] * price1 + holdings['stock2'] * price2

            # Append holdings and trades
            portfolio_values.append(total_value)
            holdings_stock1.append(holdings['stock1'])
            holdings_stock2.append(holdings['stock2'])
            units_traded_stock1.append(units_traded1)
            units_traded_stock2.append(units_traded2)

        # Assign the results back to the signals DataFrame
        signals['Portfolio Value'] = portfolio_values
        signals['Holdings ' + stock1] = holdings_stock1
        signals['Holdings ' + stock2] = holdings_stock2
        signals['Units Traded ' + stock1] = units_traded_stock1
        signals['Units Traded ' + stock2] = units_traded_stock2
        return signals

    # Step 5: Backtest pairs over the training period and collect returns
    training_pair_results = []

    for sector, pairs in final_pairs.items():
        print(f"\nBacktesting pairs over training period for sector: {sector}")
        for pair_info in pairs:
            predictor = pair_info['predictor']
            target = pair_info['target']
            hedge_ratio = pair_info['hedge_ratio']

            data = sector_data[sector][[predictor, target]]

            # Generate signals and positions over the training period
            signals = generate_signals_and_positions(predictor, target, data, window=windowsize, testing_start=training_start)

            # Backtest over the training period
            train_signals = signals[training_start:training_end]
            initial_investment = 200000

            if train_signals.empty:
                print(f"No training signals available for pair {predictor} & {target}. Skipping.")
                continue

            backtest_results = backtest(train_signals, initial_investment=initial_investment)

            # Calculate returns
            final_portfolio_value = backtest_results['Portfolio Value'].iloc[-1]
            profit = final_portfolio_value - initial_investment
            return_percentage = (profit / initial_investment) * 100

            # Collect training period results
            training_pair_results.append({
                'Sector': sector,
                'Predictor': predictor,
                'Target': target,
                'Pair': f"{predictor} & {target}",
                'Training Profit': profit,
                'Training Return (%)': return_percentage
            })

    # Convert training_pair_results to a DataFrame
    training_pair_results_df = pd.DataFrame(training_pair_results)

    # Sort the pairs based on Training Profit in descending order
    training_pair_results_df = training_pair_results_df.sort_values(by='Training Profit', ascending=False)

    # Save the training period results to a CSV file
    training_results_csv_filename = os.path.join(portfolio_value_dir, "Training_Pair_Results.csv")
    training_pair_results_df.to_csv(training_results_csv_filename, index=False)
    print(f"Training period pair results saved to {training_results_csv_filename}")

    # Plot Training Profits for all pairs
    plt.figure(figsize=(12, 8))
    plt.bar(training_pair_results_df['Pair'], training_pair_results_df['Training Profit'], color='skyblue')
    plt.title('Training Period Profits for All Pairs')
    plt.xlabel('Pair')
    plt.ylabel('Training Profit')
    plt.xticks(rotation=90)
    plt.tight_layout()
    # Save the chart
    training_profit_chart_filename = os.path.join(portfolio_value_dir, "Training_Pair_Profits.png")
    plt.savefig(training_profit_chart_filename)
    plt.close()
    print(f"Training period profit chart saved to {training_profit_chart_filename}")

    # Select the top N pairs based on Training Profit
    top_pairs_df = training_pair_results_df.head(num_pairs_to_select)
    print('Top pairs are :')
    print(top_pairs_df)
    print(top_pairs_df['Pair'])

    def create_sector_pairs_dict(top_pairs_df):
        '''This function takes a DataFrame with columns 'Sector', 'Predictor', and 'Target'and returns a dictionary with sectors as keys and a list of (Predictor, Target) tuples as values.'''
        sector_pairs = {}
        for index, row in top_pairs_df.iterrows():
            sector = row['Sector']
            predictor = row['Predictor']
            target = row['Target']

            if sector not in sector_pairs:
                sector_pairs[sector] = []
            sector_pairs[sector].append((predictor, target))

        return sector_pairs

    # Function to print the dictionary in the desired format
    def print_dict_with_single_quotes(d):
        print("{")
        for i, (sector, pairs) in enumerate(d.items()):
            print(f"    '{sector}': {pairs}", end='')
            if i < len(d) - 1:
                print(",")
            else:
                print()
        print("}")

    # Use the function to create the sector pairs dictionary
    sector_pairs_dict = create_sector_pairs_dict(top_pairs_df)

    # Print the dictionary
    print_dict_with_single_quotes(sector_pairs_dict)

    # Create a dictionary of selected pairs mapped by sector
    selected_pairs = {}

    for idx, row in top_pairs_df.iterrows():
        sector = row['Sector']
        predictor = row['Predictor']
        target = row['Target']

        if sector not in selected_pairs:
            selected_pairs[sector] = []

        # Find the pair_info from final_pairs[sector]
        for pair_info in final_pairs[sector]:
            if pair_info['predictor'] == predictor and pair_info['target'] == target:
                selected_pairs[sector].append(pair_info)
                break

    # Initialize variables to accumulate profits and returns
    sector_profits = {}
    sector_returns = {}
    total_profit = 0
    total_return = 0

    # Initialize a list to store pair results
    pair_results = []

    # Initialize a list to store ongoing positions
    ongoing_positions = []

    # Step 7: Execute backtest and plot results for each sector using selected_pairs
    for sector, pairs in selected_pairs.items():
        sector_profit = 0
        sector_return = 0
        sector_portfolio_values = pd.Series(dtype='float64')  # To accumulate portfolio values over time
        print(f"\nProcessing sector: {sector}")
        for pair_info in pairs:
            predictor = pair_info['predictor']
            target = pair_info['target']
            hedge_ratio = pair_info['hedge_ratio']

            data = sector_data[sector][[predictor, target]]

            signals = generate_signals_and_positions(predictor, target, data, window=windowsize, testing_start=testing_start)
            print('Predictor and target')
            print(predictor, target)
            # Backtest over the test period
            test_signals = signals[testing_start:testing_end]
            initial_investment = 200000  # 100000 per asset

            if test_signals.empty:
                print(f"No test signals available for pair {predictor} & {target}. Skipping.")
                continue

            backtest_results = backtest(test_signals, initial_investment=initial_investment)

            # Save the backtest results (which include Portfolio Value) to the portfolio_value_dir
            pair_dir = os.path.join(portfolio_value_dir, f"{predictor}_{target}")
            os.makedirs(pair_dir, exist_ok=True)
            portfolio_value_filename = os.path.join(pair_dir, f"{predictor}_{target}_portfolio_value.csv")
            backtest_results.to_csv(portfolio_value_filename)
            print(f"Portfolio value data saved to {portfolio_value_filename}")

            # Calculate returns
            final_portfolio_value = backtest_results['Portfolio Value'].iloc[-1]
            profit = final_portfolio_value - initial_investment
            return_percentage = (profit / initial_investment) * 100

            # Compute maximum drawdown
            max_drawdown = calculate_max_drawdown(backtest_results['Portfolio Value']) * 100  # In percentage

            print(f"Pair: {predictor} & {target}")
            print(f"Final Portfolio Value: {final_portfolio_value:.2f}")
            print(f"Profit: {profit:.2f}")
            print(f"Return: {return_percentage:.2f}%")
            print(f"Maximum Drawdown: {max_drawdown:.2f}%\n")

            # Accumulate sector profit and return
            sector_profit += profit
            sector_return += return_percentage

            # Accumulate sector portfolio values over time
            # Align the indices to handle any missing dates
            if sector_portfolio_values.empty:
                sector_portfolio_values = backtest_results['Portfolio Value']
            else:
                sector_portfolio_values = sector_portfolio_values.add(backtest_results['Portfolio Value'], fill_value=0)

            # Collect pair results for CSV
            pair_results.append({
                'Sector': sector,
                'Predictor': predictor,
                'Target': target,
                'Pair': f"{predictor} & {target}",
                'Profit': profit,
                'Return (%)': return_percentage,
                'Max Drawdown (%)': max_drawdown,
                'Training Start Date': training_start,
                'Training End Date': training_end,
                'Testing Start Date': testing_start,
                'Testing End Date': testing_end
            })

            # Check for ongoing positions at the end of the backtest
            holdings_stock1 = backtest_results['Holdings ' + predictor].iloc[-1]
            holdings_stock2 = backtest_results['Holdings ' + target].iloc[-1]

            if holdings_stock1 != 0 or holdings_stock2 != 0:
                # There is an open position
                # Find the last position change date
                position_change_indices = backtest_results[(backtest_results['positions1'] != 0) | (backtest_results['positions2'] != 0)].index
                if len(position_change_indices) > 0:
                    last_position_change_date = position_change_indices[-1]
                else:
                    # No position changes, maybe initial position, set to first date
                    last_position_change_date = backtest_results.index[0]
                
                # Entry prices
                entry_price_stock1 = backtest_results.loc[last_position_change_date, predictor]
                entry_price_stock2 = backtest_results.loc[last_position_change_date, target]
                
                # Current prices
                current_price_stock1 = backtest_results[predictor].iloc[-1]
                current_price_stock2 = backtest_results[target].iloc[-1]
                
                # Compute profit or loss on the open position
                pnl_stock1 = holdings_stock1 * (current_price_stock1 - entry_price_stock1)
                pnl_stock2 = holdings_stock2 * (current_price_stock2 - entry_price_stock2)
                
                total_pnl = pnl_stock1 + pnl_stock2
                
                # Side (long or short)
                position_stock1 = 'Long' if holdings_stock1 > 0 else 'Short' if holdings_stock1 < 0 else 'Flat'
                position_stock2 = 'Long' if holdings_stock2 > 0 else 'Short' if holdings_stock2 < 0 else 'Flat'
                
                # Collect details
                ongoing_positions.append({
                    'Sector': sector,
                    'Predictor': predictor,
                    'Target': target,
                    'Pair': f"{predictor} & {target}",
                    'Position Entry Date': last_position_change_date.strftime('%Y-%m-%d'),
                    'Current Holdings Predictor': holdings_stock1,
                    'Current Holdings Target': holdings_stock2,
                    'Position in Predictor': position_stock1,
                    'Position in Target': position_stock2,
                    'Entry Price Predictor': entry_price_stock1,
                    'Entry Price Target': entry_price_stock2,
                    'Current Price Predictor': current_price_stock1,
                    'Current Price Target': current_price_stock2,
                    'PnL Predictor': pnl_stock1,
                    'PnL Target': pnl_stock2,
                    'Total PnL': total_pnl,
                })

            # Plot Portfolio Value
            plt.figure(figsize=(10, 6))
            plt.plot(backtest_results.index, backtest_results['Portfolio Value'])
            plt.title(f'Portfolio Value for Pair: {predictor} - {target}')
            plt.xlabel('Date')
            plt.ylabel('Portfolio Value')
            plt.grid(True)
            
            # Save the portfolio value plot to portfolio_value_dir
            portfolio_chart_filename = os.path.join(pair_dir, f"{predictor}_{target}_portfolio_value.png")
            plt.savefig(portfolio_chart_filename)
            plt.close()
            print(f"Portfolio value chart saved to {portfolio_chart_filename}")

            # Plot the Z-score and Positions for Test Period
            plt.figure(figsize=(14, 7))

            # Z-Score and Positions Plot
            ax1 = plt.subplot(1, 1, 1)
            ax1.plot(test_signals.index, test_signals['Z-Score'], label='Z-Score', color='black')
            ax1.axhline(1, color='red', linestyle='--', label='Upper Limit')
            ax1.axhline(-1, color='green', linestyle='--', label='Lower Limit')
            ax1.set_ylabel('Z-Score')
            ax1.set_title(f'Z-Score and Positions for Pair: {predictor} - {target} (Test Period)')
            ax1.grid(True)

            # Overlay positions using a secondary y-axis
            ax2 = ax1.twinx()
            ax2.step(test_signals.index, test_signals['positions1'], where='post', label=f'Positions for {predictor}', color='blue', linewidth=1.5)
            ax2.step(test_signals.index, test_signals['positions2'], where='post', label=f'Positions for {target}', color='purple', linewidth=1.5)
            ax2.set_ylabel('Positions')
            ax2.set_ylim(-2, 2)  # Adjust y-axis limits for positions (-1, 0, 1)

            # Combine legends from both axes
            lines_1, labels_1 = ax1.get_legend_handles_labels()
            lines_2, labels_2 = ax2.get_legend_handles_labels()
            ax2.legend(lines_1 + lines_2, labels_1 + labels_2, loc='upper left')

            plt.tight_layout()
            
            # Save the Z-score and positions plot to zscore_data_dir
            zscore_positions_dir = os.path.join(zscore_data_dir, f"{predictor}_{target}")
            os.makedirs(zscore_positions_dir, exist_ok=True)
            zscore_positions_filename = os.path.join(zscore_positions_dir, f"{predictor}_{target}_zscore_positions.png")
            plt.savefig(zscore_positions_filename)
            plt.close()
            print(f"Z-score and positions chart saved to {zscore_positions_filename}")

        # Store sector profit and return
        sector_profits[sector] = sector_profit
        sector_returns[sector] = sector_return

        # Accumulate total profit and return
        total_profit += sector_profit
        total_return += sector_return

        # Plot cumulative sector portfolio value over time
        if not sector_portfolio_values.empty:
            plt.figure(figsize=(10, 6))
            plt.plot(sector_portfolio_values.index, sector_portfolio_values.values)
            plt.title(f'Cumulative Portfolio Value for Sector: {sector}')
            plt.xlabel('Date')
            plt.ylabel('Cumulative Portfolio Value')
            plt.grid(True)
            
            # Save the cumulative sector portfolio value plot
            sanitized_sector = sanitize_filename(sector)
            sector_portfolio_chart_filename = os.path.join(portfolio_value_dir, f"{sanitized_sector}_cumulative_portfolio_value.png")
            plt.savefig(sector_portfolio_chart_filename)
            plt.close()
            print(f"Cumulative portfolio value chart for sector {sector} saved to {sector_portfolio_chart_filename}")

    # After processing all sectors, print and plot cumulative profits and returns
    print("\nCumulative Profit per Sector:")
    for sector in sector_profits:
        print(f"Sector: {sector}, Profit: {sector_profits[sector]:.2f}, Return: {sector_returns[sector]:.2f}%")

    print(f"\nTotal Profit across all sectors: {total_profit:.2f}")
    print(f"Total Return across all sectors: {total_return:.2f}%")

    # Plot cumulative profits per sector
    plt.figure(figsize=(10, 6))
    sectors_list = list(sector_profits.keys())
    profits_list = [sector_profits[sector] for sector in sectors_list]
    plt.bar(sectors_list, profits_list, color='skyblue')
    plt.title('Cumulative Profit per Sector')
    plt.xlabel('Sector')
    plt.ylabel('Profit')
    plt.grid(True)
    plt.xticks(rotation=90)

    # Annotate the total profit
    plt.text(0.5, max(profits_list)*0.9, f"Total Profit: {total_profit:.2f}", fontsize=12, ha='center')

    # Save the cumulative profit per sector plot
    cumulative_profit_chart_filename = os.path.join(portfolio_value_dir, "Cumulative_Profit_per_Sector.png")
    plt.savefig(cumulative_profit_chart_filename)
    plt.close()
    print(f"Cumulative profit per sector chart saved to {cumulative_profit_chart_filename}")

    # Initialize a DataFrame to accumulate sector portfolio values
    all_sector_portfolios = pd.DataFrame()

    for sector, pairs in selected_pairs.items():
        sector_portfolio_values = pd.Series(dtype='float64')
        for pair_info in pairs:
            predictor = pair_info['predictor']
            target = pair_info['target']
            pair_dir = os.path.join(portfolio_value_dir, f"{predictor}_{target}")
            portfolio_value_filename = os.path.join(pair_dir, f"{predictor}_{target}_portfolio_value.csv")
            backtest_results = pd.read_csv(portfolio_value_filename, index_col=0, parse_dates=True)
            if sector_portfolio_values.empty:
                sector_portfolio_values = backtest_results['Portfolio Value']
            else:
                sector_portfolio_values = sector_portfolio_values.add(backtest_results['Portfolio Value'], fill_value=0)
        # Add sector portfolio values to the DataFrame
        all_sector_portfolios[sector] = sector_portfolio_values

    # Plot cumulative portfolio values over time for all sectors
    plt.figure(figsize=(12, 8))
    for sector in all_sector_portfolios.columns:
        plt.plot(all_sector_portfolios.index, all_sector_portfolios[sector], label=sector)
    plt.title('Cumulative Portfolio Values per Sector Over Time')
    plt.xlabel('Date')
    plt.ylabel('Cumulative Portfolio Value')
    plt.legend()
    plt.grid(True)

    # Save the cumulative portfolio values plot
    cumulative_portfolio_chart_filename = os.path.join(portfolio_value_dir, "Cumulative_Portfolio_Values_per_Sector.png")
    plt.savefig(cumulative_portfolio_chart_filename)
    plt.close()
    print(f"Cumulative portfolio values per sector chart saved to {cumulative_portfolio_chart_filename}")

    # Save the pair results to a CSV file
    pair_results_df = pd.DataFrame(pair_results)
    csv_filename = os.path.join(portfolio_value_dir, "Pair_Results.csv")
    pair_results_df.to_csv(csv_filename, index=False)
    print(f"Pair results saved to {csv_filename}")

    # Save the ongoing positions to a CSV file
    if ongoing_positions:
        ongoing_positions_df = pd.DataFrame(ongoing_positions)
        ongoing_positions_filename = os.path.join(portfolio_value_dir, "Ongoing_Positions.csv")
        ongoing_positions_df.to_csv(ongoing_positions_filename, index=False)
        print(f"Ongoing positions saved to {ongoing_positions_filename}")
    else:
        print("No ongoing positions to save.")
