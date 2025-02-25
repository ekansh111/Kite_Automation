"""
This script implements a pairs trading strategy based on mean reversion.

**Main Functionalities:**

1. **Data Preparation:**
   - Downloads historical stock data for specified cointegrated pairs from Yahoo Finance using `yfinance`.
   - Organizes the data by sectors.

2. **Cointegration Analysis:**
   - Performs OLS regression to determine hedge ratios for the pairs.
   - Checks statistical criteria such as t-tests, F-tests, Durbin-Watson statistic, Jarque-Bera test, and stationarity of residuals using the Augmented Dickey-Fuller test.
   - Plots spread and residuals for each pair.

3. **Signal Generation:**
   - Generates trading signals based on Z-scores of the price ratios.
   - Defines entry and exit signals based on upper and lower Z-score thresholds.

4. **Backtesting:**
   - Simulates trades over a training and testing period.
   - Implements risk management by calculating drawdowns and stopping trading if a maximum drawdown threshold is exceeded.
   - Calculates portfolio values over time.

5. **Performance Evaluation:**
   - Calculates profits, returns, and maximum drawdowns for each pair and sector.
   - Aggregates results and plots cumulative profits per sector.

6. **Reporting:**
   - Saves various data and plots to directories, including closing prices, spread and residuals, portfolio values, and Z-score data.
   - Sends an email with ongoing positions data if any positions are still open at the end of the backtest.

**Usage:**

- Update the `cointegrated_pairs` dictionary with the desired pairs for each sector.
- Adjust the date ranges for total data, training, and testing periods.
- Run the script to perform the analysis and backtesting.
- Ensure that the required directories specified in `Directories.py` exist or adjust paths accordingly.

**Dependencies:**

- Python 3.x
- Libraries: `pandas`, `numpy`, `yfinance`, `statsmodels`, `matplotlib`, `seaborn`, `datetime`, `os`, `re`, `itertools`
- Custom Modules:
  - `Push_File_To_Email` (contains the `send_email` function)
  - `Delete_Mean_Reverting_Data` (contains the `delete_contents_in_directories` function)
  - `Directories` (contains directory paths)

**Notes:**

- The script uses the 'Agg' backend for matplotlib to allow non-interactive plotting (useful when running on a server without display).
- The script assumes that the cointegrated pairs are already known.
- The script includes risk management by stopping trading for a pair if the maximum drawdown threshold is exceeded during testing.
- Ongoing positions at the end of the backtest are emailed using the `send_email` function.

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
from Push_File_To_Email import send_email
from Directories import *

#To clear out the directory of junk files first
from  Delete_Mean_Reverting_Data import delete_contents_in_directories

#To clear out the directory of junk files first
delete_contents_in_directories()

# Define the sanitize_filename function
def sanitize_filename(s):
    # Replace spaces with underscores
    s = s.replace(' ', '_')
    # Replace ampersand with 'and'
    s = s.replace('&', 'and')
    # Remove invalid characters
    s = re.sub(r'[<>:"/\\|?*]', '', s)
    return s

# Replace the sectors and their stocks with the provided cointegrated pairs
cointegrated_pairs = {
    'Realty': [('OBEROIRLTY.NS', 'DLF.NS')],
    'Automobiles & Auto Components': [('APOLLOTYRE.NS', 'MOTHERSON.NS'), ('EICHERMOT.NS', 'MOTHERSON.NS')],
    'Chemicals & Petrochemicals': [('ATUL.NS', 'AARTIIND.NS'), ('PIDILITIND.NS', 'AARTIIND.NS')],
    'Oil & Gas': [('PETRONET.NS', 'BPCL.NS')],
    'Software & Services': [('NAUKRI.NS', 'TCS.NS'), ('INFY.NS', 'HCLTECH.NS')],
    'Metals & Mining': [('HINDCOPPER.NS', 'SAIL.NS'), ('HINDALCO.NS', 'TATASTEEL.NS'), ('NMDC.NS', 'HINDCOPPER.NS')],
    'banking_companies': [('KOTAKBANK.NS', 'HDFCBANK.NS')],
    'FMCG': [('NESTLEIND.NS', 'DABUR.NS'), ('HINDUNILVR.NS', 'DABUR.NS'), ('COLPAL.NS', 'DABUR.NS'), ('HINDUNILVR.NS', 'COLPAL.NS')],       
    'Pharmaceuticals & Biotechnology': [('DIVISLAB.NS', 'TORNTPHARM.NS')],
    'Cement and Construction': [('ACC.NS', 'AMBUJACEM.NS'), ('JKCEMENT.NS', 'GMRINFRA.NS')],
    'Consumer Durables': [('HAVELLS.NS', 'VOLTAS.NS')],
    'General Industrials': [('SIEMENS.NS', 'ABB.NS')]
}

# Total date range for data downloading
total_start_date = '2005-01-04'
total_end_date = (datetime.today() + timedelta(days=1)).strftime('%Y-%m-%d')  # Updated to tomorrow's date
#total_end_date = (datetime.today()).strftime('%Y-%m-%d')# Updated to today's date

# Training and testing dates
training_start = '2015-01-02'
training_end = '2021-12-31'
#testing_start = '2022-01-03'
testing_start = '2024-10-01'
testing_end = (datetime.today() ).strftime('%Y-%m-%d') # Updated to today's date

windowsize = 252
top_pairs_to_select = 100

# Convert date strings to Timestamps
training_start = pd.to_datetime(training_start)
training_end = pd.to_datetime(training_end)
testing_start = pd.to_datetime(testing_start)
testing_end = pd.to_datetime(testing_end)

# Directories to save data and charts
base_dir = MeanReversionCharts 
closing_prices_dir = os.path.join(base_dir, "Closing Prices")
spread_residuals_dir = os.path.join(base_dir, "Spread and Residuals")
portfolio_value_dir = os.path.join(base_dir, "Portfolio Value")
zscore_data_dir = os.path.join(base_dir, "z score")

# Ensure the directories exist
os.makedirs(base_dir, exist_ok=True)
os.makedirs(closing_prices_dir, exist_ok=True)
os.makedirs(spread_residuals_dir, exist_ok=True)
os.makedirs(portfolio_value_dir, exist_ok=True)
os.makedirs(zscore_data_dir, exist_ok=True)

# Function to download stock data
def download_stock_data(tickers, start_date, end_date):
    #end_date = '2024-11-12'
    stock_data = {}
    valid_tickers = []
    for ticker in tickers:
        try:
            data = yf.download(ticker, start=start_date, end=end_date)
            if data.empty:
                print(f"No data for {ticker}. Skipping.")
                continue
            stock_data[ticker] = data['Close']#[ticker]
            valid_tickers.append(ticker)
        except Exception as e:
            print(f"Error downloading {ticker}: {e}")
            continue
    stock_df = pd.DataFrame(stock_data)
    return stock_df, valid_tickers

# Prepare a set of all tickers involved in the cointegrated pairs
all_tickers = set()
for pairs in cointegrated_pairs.values():
    for pair in pairs:
        all_tickers.update([pair[0], pair[1]])

# Step 1: Download data for all tickers involved in the cointegrated pairs
print("Downloading data for all tickers involved in the cointegrated pairs.")
#print('end date ' + str(total_end_date))
data, valid_tickers = download_stock_data(list(all_tickers), total_start_date, total_end_date)
print(data,valid_tickers)
if data.empty or len(valid_tickers) < 2:
    print(f"Not enough valid tickers after download. Exiting script.")
    exit()

# Organize data per sector
sector_data = {}
for sector in cointegrated_pairs.keys():
    # Extract tickers for the sector
    sector_tickers = set()
    for pair in cointegrated_pairs[sector]:
        sector_tickers.update([pair[0], pair[1]])
    # Get data for sector tickers
    sector_data[sector] = data[list(sector_tickers)]

# Proceed with the rest of the script using the provided cointegrated pairs
# Step 4: Build OLS regression models with the cointegrated pairs

from statsmodels.stats.stattools import durbin_watson

final_pairs = {}
for sector, pairs in cointegrated_pairs.items():
    print(f"Analyzing OLS regression for sector: {sector}")
    valid_pairs = []
    for stock1, stock2 in pairs:
        # Check if both stocks are in the downloaded data
        if stock1 not in data.columns or stock2 not in data.columns:
            print(f"Data for pair {stock1} & {stock2} not available. Skipping.")
            continue

        # Select the predictor as the stock with higher mean close price in training data
        try:
            mean1 = data[stock1][training_start:training_end].mean()
            mean2 = data[stock2][training_start:training_end].mean()
            if mean1 > mean2:
                predictor = stock1
                target = stock2
            else:
                predictor = stock2
                target = stock1

            X = sm.add_constant(data[predictor][training_start:training_end])
            y = data[target][training_start:training_end]
            model = sm.OLS(y, X).fit()

            # Extract statistical outputs
            hedge_ratio = model.params.iloc[1]
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
            spread = data[predictor][training_start:training_end] - \
                     (hedge_ratio * data[target][training_start:training_end])
            
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
            sanitized_sector = sanitize_filename(sector)
            spread_filename = os.path.join(spread_residuals_dir, f"{sanitized_sector}_{predictor}_{target}_spread.png")
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
            residuals_filename = os.path.join(spread_residuals_dir, f"{sanitized_sector}_{predictor}_{target}_residuals.png")
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
            residuals_vs_spread_filename = os.path.join(spread_residuals_dir, f"{sanitized_sector}_{predictor}_{target}_residuals_vs_spread.png")
            plt.savefig(residuals_vs_spread_filename)
            plt.close()
            print(f"Residuals vs Spread chart saved to {residuals_vs_spread_filename}")
            
            # For the purpose of this script, we proceed with all pairs
            valid_pairs.append({
                'predictor': predictor,
                'target': target,
                'hedge_ratio': hedge_ratio,
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

        # **Update positions on the first day of the testing period**
        if testing_start is not None:
            # Convert testing_start to Timestamp for index alignment
            testing_start_date = pd.to_datetime(testing_start)

            # Check if the testing_start_date is in the index
            if testing_start_date in combined.index:
                first_test_idx = combined.index.get_loc(testing_start_date)
                # For positions1 and positions2, no need to adjust as we handle positions in backtest
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

    def backtest(signals, initial_investment, max_drawdown_threshold):
        z_scores = signals['Z-Score']
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
        positions1_list = []
        positions2_list = []

        # Initialize variables for new logic
        is_active = True  # Indicates whether we can trade
        position_open = False  # Indicates whether we have an open position
        position_entry_value = None  # Portfolio value at position entry
        max_portfolio_value_since_entry = None  # Max portfolio value since position entry
        trading_stopped = False  # Indicates whether trading is permanently stopped

        for i in range(len(signals)):
            date = signals.index[i]
            price1 = signals.iloc[i][stock1]
            price2 = signals.iloc[i][stock2]
            signal1 = signals['signals1'].iloc[i]
            signal2 = signals['signals2'].iloc[i]
            z_score = z_scores.iloc[i]

            units_traded1 = 0
            units_traded2 = 0

            # Update total portfolio value considering short positions
            total_value = cash + holdings['stock1'] * price1 + holdings['stock2'] * price2

            if position_open:
                # Update max_portfolio_value_since_entry
                if max_portfolio_value_since_entry is None or total_value > max_portfolio_value_since_entry:
                    max_portfolio_value_since_entry = total_value

                # Calculate drawdown
                drawdown = (total_value - max_portfolio_value_since_entry) / max_portfolio_value_since_entry
                #print('drawdown details')
                #print(drawdown)
                #print(max_drawdown_threshold)
                if drawdown <= -max_drawdown_threshold:
                    print(f"Maximum drawdown exceeded on {date}. Exiting positions and permanently stopping trading for this pair.")
                    # Exit positions
                    # For stock1
                    if holdings['stock1'] != 0:
                        cash += holdings['stock1'] * price1
                        units_traded1 = -holdings['stock1']  # Negative units if selling
                        holdings['stock1'] = 0
                    # For stock2
                    if holdings['stock2'] != 0:
                        cash += holdings['stock2'] * price2
                        units_traded2 = -holdings['stock2']  # Negative units if selling
                        holdings['stock2'] = 0

                    # Reset variables
                    position_open = False
                    position_entry_value = None
                    max_portfolio_value_since_entry = None
                    trading_stopped = True  # Permanently stop trading for this pair

            if not trading_stopped:
                if is_active:
                    if not position_open:
                        # No open position, check for signals to enter positions
                        if signal1 != 0:
                            # Enter position
                            # For asset1
                            if signal1 == 1:
                                # Enter long position
                                max_units = (cash / 2) // price1
                                holdings['stock1'] += max_units
                                cash -= max_units * price1
                                units_traded1 = max_units
                            elif signal1 == -1:
                                # Enter short position
                                max_units = (cash / 2) // price1
                                holdings['stock1'] -= max_units
                                cash += max_units * price1
                                units_traded1 = -max_units

                            # For asset2
                            if signal2 == 1:
                                # Enter long position
                                max_units = (cash / 2) // price2
                                holdings['stock2'] += max_units
                                cash -= max_units * price2
                                units_traded2 = max_units
                            elif signal2 == -1:
                                # Enter short position
                                max_units = (cash / 2) // price2
                                holdings['stock2'] -= max_units
                                cash += max_units * price2
                                units_traded2 = -max_units

                            # Initialize position variables
                            position_open = True
                            position_entry_value = total_value
                            max_portfolio_value_since_entry = total_value

                    else:
                        # Position is open, check if signals indicate to exit positions
                        if signal1 == 0 and signal2 == 0:
                            # Signals indicate to exit positions
                            # For asset1
                            if holdings['stock1'] != 0:
                                cash += holdings['stock1'] * price1
                                units_traded1 = -holdings['stock1']
                                holdings['stock1'] = 0
                            # For asset2
                            if holdings['stock2'] != 0:
                                cash += holdings['stock2'] * price2
                                units_traded2 = -holdings['stock2']
                                holdings['stock2'] = 0

                            # Reset position variables
                            position_open = False
                            position_entry_value = None
                            max_portfolio_value_since_entry = None
                else:
                    # is_active == False
                    # We do not process any new signals
                    # Check if z-score returns between -1 and 1
                    if -1 < z_score < 1:
                        is_active = True  # We can start trading again
                        print(f"Z-score returned to between -1 and 1 on {date}. Resuming trading for this pair.")
            else:
                # Trading has been permanently stopped for this pair
                pass  # Do nothing

            # Update total portfolio value considering short positions
            total_value = cash + holdings['stock1'] * price1 + holdings['stock2'] * price2
            portfolio_values.append(total_value)
            holdings_stock1.append(holdings['stock1'])
            holdings_stock2.append(holdings['stock2'])
            units_traded_stock1.append(units_traded1)
            units_traded_stock2.append(units_traded2)

            # Record positions
            position1 = 1 if holdings['stock1'] > 0 else -1 if holdings['stock1'] < 0 else 0
            position2 = 1 if holdings['stock2'] > 0 else -1 if holdings['stock2'] < 0 else 0
            positions1_list.append(position1)
            positions2_list.append(position2)

        signals['Portfolio Value'] = portfolio_values
        signals['Holdings ' + stock1] = holdings_stock1
        signals['Holdings ' + stock2] = holdings_stock2
        signals['Units Traded ' + stock1] = units_traded_stock1
        signals['Units Traded ' + stock2] = units_traded_stock2
        signals['positions1'] = positions1_list
        signals['positions2'] = positions2_list

        return signals


    # Initialize variables to accumulate profits and returns
    sector_profits = {}
    sector_returns = {}
    total_profit = 0
    total_return = 0

    # Initialize a list to store pair results
    pair_results = []

    # Initialize a list to store ongoing positions
    ongoing_positions = []

    # Dictionary to store maximum drawdown during training for each pair
    training_drawdowns = {}

    # Step 7: Execute backtest and plot results for each sector
    for sector, pairs in final_pairs.items():
        sector_profit = 0
        sector_return = 0
        sector_portfolio_values = pd.Series(dtype='float64')  # To accumulate portfolio values over time
        print(f"\nProcessing sector: {sector}")
        for pair_info in pairs:
            predictor = pair_info['predictor']
            target = pair_info['target']
            hedge_ratio = pair_info['hedge_ratio']

            # Ensure data exists for both stocks
            if predictor not in data.columns or target not in data.columns:
                print(f"Data for pair {predictor} & {target} not available. Skipping.")
                continue

            data_pair = data[[predictor, target]]

            # Generate signals for the entire period
            signals = generate_signals_and_positions(predictor, target, data_pair, window=windowsize)

            # Backtest over the training period
            print(f"Backtesting during training period for pair: {predictor} & {target}")
            training_signals = signals[training_start:training_end]
            initial_investment = 200000  # 100000 per asset

            if training_signals.empty:
                print(f"No training signals available for pair {predictor} & {target}. Skipping.")
                continue

            # Use a large max_drawdown_threshold during training to ensure trading doesn't stop
            training_backtest_results = backtest(training_signals, initial_investment=initial_investment, max_drawdown_threshold=1.0)

            # Compute maximum drawdown during training
            training_max_drawdown = calculate_max_drawdown(training_backtest_results['Portfolio Value'])

            # Save the maximum drawdown value for this pair
            training_drawdowns[(predictor, target)] = training_max_drawdown

            print(f"Maximum Drawdown during training for pair {predictor} & {target}: {training_max_drawdown*100:.2f}%\n")

            # Save the training backtest results
            pair_dir = os.path.join(portfolio_value_dir, f"{predictor}_{target}")
            os.makedirs(pair_dir, exist_ok=True)
            training_portfolio_value_filename = os.path.join(pair_dir, f"{predictor}_{target}_training_portfolio_value.csv")
            training_backtest_results.to_csv(training_portfolio_value_filename)
            print(f"Training portfolio value data saved to {training_portfolio_value_filename}")

            # **Plot Portfolio Value During Training Period**
            plt.figure(figsize=(10, 6))
            plt.plot(training_backtest_results.index, training_backtest_results['Portfolio Value'])
            plt.title(f'Training Portfolio Value for Pair: {predictor} - {target}')
            plt.xlabel('Date')
            plt.ylabel('Portfolio Value')
            plt.grid(True)

            # Save the training portfolio value plot
            training_portfolio_chart_filename = os.path.join(pair_dir, f"{predictor}_{target}_training_portfolio_value.png")
            plt.savefig(training_portfolio_chart_filename)
            plt.close()
            print(f"Training portfolio value chart saved to {training_portfolio_chart_filename}")

            # Backtest over the testing period
            print(f"Backtesting during testing period for pair: {predictor} & {target}")
            test_signals = signals[testing_start:testing_end]

            if test_signals.empty:
                print(f"No test signals available for pair {predictor} & {target}. Skipping.")
                continue

            # Use the maximum drawdown from training as the threshold during testing
            backtest_results = backtest(test_signals, initial_investment=initial_investment, max_drawdown_threshold=training_max_drawdown)

            # Save the backtest results (which include Portfolio Value) to the portfolio_value_dir
            portfolio_value_filename = os.path.join(pair_dir, f"{predictor}_{target}_testing_portfolio_value.csv")
            backtest_results.to_csv(portfolio_value_filename)
            print(f"Testing portfolio value data saved to {portfolio_value_filename}")

            # Calculate returns
            final_portfolio_value = backtest_results['Portfolio Value'].iloc[-1]
            profit = final_portfolio_value - initial_investment
            return_percentage = (profit / initial_investment) * 100

            # Compute maximum drawdown during testing
            max_drawdown = calculate_max_drawdown(backtest_results['Portfolio Value']) * 100  # In percentage

            print(f"Pair: {predictor} & {target}")
            print(f"Final Portfolio Value: {final_portfolio_value:.2f}")
            print(f"Profit: {profit:.2f}")
            print(f"Return: {return_percentage:.2f}%")
            print(f"Maximum Drawdown during testing: {max_drawdown:.2f}%\n")

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
                'Max Drawdown during Training (%)': training_max_drawdown * 100,
                'Max Drawdown during Testing (%)': max_drawdown,
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
                position_change_indices = backtest_results[(backtest_results['positions1'].diff() != 0) | (backtest_results['positions2'].diff() != 0)].index
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


            # Plot Portfolio Value during Testing Period
            plt.figure(figsize=(10, 6))
            plt.plot(backtest_results.index, backtest_results['Portfolio Value'])
            plt.title(f'Testing Portfolio Value for Pair: {predictor} - {target}')
            plt.xlabel('Date')
            plt.ylabel('Portfolio Value')
            plt.grid(True)
            
            # Save the portfolio value plot to portfolio_value_dir
            portfolio_chart_filename = os.path.join(pair_dir, f"{predictor}_{target}_testing_portfolio_value.png")
            plt.savefig(portfolio_chart_filename)
            plt.close()
            print(f"Testing portfolio value chart saved to {portfolio_chart_filename}")

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
    plt.xticks(rotation=90)
    plt.grid(True)

    # Annotate the total profit
    plt.text(0.5, max(profits_list)*0.9, f"Total Profit: {total_profit:.2f}", fontsize=12, ha='center')

    # Save the cumulative profit per sector plot
    cumulative_profit_chart_filename = os.path.join(portfolio_value_dir, "Cumulative_Profit_per_Sector.png")
    plt.savefig(cumulative_profit_chart_filename)
    plt.close()
    print(f"Cumulative profit per sector chart saved to {cumulative_profit_chart_filename}")

    # Save the pair results to a CSV file
    pair_results_df = pd.DataFrame(pair_results)
    csv_filename = os.path.join(portfolio_value_dir, "Pair_Results.csv")
    pair_results_df.to_csv(csv_filename, index=False)
    print(f"Pair results saved to {csv_filename}")

    # Save the ongoing positions to a CSV file
    if ongoing_positions:
        ongoing_positions_df = pd.DataFrame(ongoing_positions)
        send_email(ongoing_positions_df)
        ongoing_positions_filename = os.path.join(portfolio_value_dir, "Ongoing_Positions.csv")
        ongoing_positions_df.to_csv(ongoing_positions_filename, index=False)
        print(f"Ongoing positions saved to {ongoing_positions_filename}")
    else:
        print("No ongoing positions to save.")
