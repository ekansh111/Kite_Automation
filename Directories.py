import datetime

KiteEkanshLogin = 'C:/Users/ekans/OneDrive/Documents/inputs/Login_Credentials.txt'
KiteEkanshLoginAPIKey = 'C:/Users/ekans/OneDrive/Documents/inputs/api_key_IK.txt'
KiteEkanshLoginAccessToken = 'C:/Users/ekans/OneDrive/Documents/inputs/access_token_IK.txt'
ZerodhaInstrumentDirectory = "ZerodhaInstruments.csv" 

KiteRashmiLogin = 'C:/Users/ekans/OneDrive/Documents/inputs/Login_Credentials_YD6016.txt'
KiteRashmiLoginAccessToken = 'C:/Users/ekans/OneDrive/Documents/inputs/access_token_YD.txt'

AngelEkanshLoginCred = 'C:/Users/ekans/OneDrive/Documents/inputs/Login_Credentials_Angel.txt'
AngelNararushLoginCred = 'Login_Credentials_Angel_Dad.txt' 
AngelInstrumentDirectory = 'AngelInstrumentDetails.csv'

WriteOptionDetailsFile = 'C:/Users/ekans/OneDrive/Documents/inputs/option_details.csv'
WriteAllContractDet = 'C:/Users/ekans/OneDrive/Documents/inputs/TEXT_INSTRUMENTS.csv'

WorkDirectory = 'C:/Users/ekans/OneDrive/Documents/inputs/'

IntraDayDirectory = r"C:\Users\ekans\OneDrive\Documents\Trading\IntraDay_Stocks_Selector"
IntraDayDirectoryHistory = r"C:\Users\ekans\OneDrive\Documents\Trading\IntraDay_Stocks_Selector\History4"
Nifty500ConstituentList = r"C:\Users\ekans\OneDrive\Documents\Trading\IntraDay_Stocks_Selector\ind_nifty500list.csv"
PositionDataOpDirectory = r"C:\Users\ekans\OneDrive\Documents\Trading\PositionsData"
Nifty500MACDDailyData = r"Nifty500DailyMACDData.csv"

MeanReversionCharts = r"C:\Users\ekans\OneDrive\Documents\Trading\Scripts\Charts"
MeanReversionPortfolioValue = r"C:\Users\ekans\OneDrive\Documents\Trading\Scripts\Charts\Portfolio Value"
MeanReversionZScore = r"C:\Users\ekans\OneDrive\Documents\Trading\Scripts\Charts\z score"
MeanReversionSpreadResiduals = r"C:\Users\ekans\OneDrive\Documents\Trading\Scripts\Charts\Spread and Residuals"
MeanReversionClosingPrice = r"C:\Users\ekans\OneDrive\Documents\Trading\Scripts\Charts\Closing Prices"
MeanReversionCointigrationHeatMap = r"C:\Users\ekans\OneDrive\Documents\Trading\Scripts\Charts\Cointegration Heatmaps"

# Get the current date
today = datetime.date.today()
# Format the month as a three-letter abbreviation in uppercase (e.g. 'APR')
month_str = today.strftime("%b").upper()
# Format the year in YYYY format
year_str = today.strftime("%Y")
# Combine them (e.g. 'APR2025')
month_year_dir = f"{month_str}{year_str}"

RelativeMomentumDirectoryReturns = rf'C:\Users\ekans\OneDrive\Documents\Trading\Momentum Stock Investing Data\{month_year_dir}\relative momentum returns.csv'
AbsoluteMomentumOutputDirectory = rf'C:\Users\ekans\OneDrive\Documents\Trading\Momentum Stock Investing Data\{month_year_dir}\absolute momentum returns.csv'
MomentumFilesOutputDirectory = rf"C:\Users\ekans\OneDrive\Documents\Trading\Momentum Stock Investing Data\{month_year_dir}"
Nifty500ConstituentFilePath = rf'C:\Users\ekans\OneDrive\Documents\Trading\Momentum Stock Investing Data\{month_year_dir}\ind_nifty500list.csv'

SystemLocalIp = '192.168.0.194'
SystemPublicIp = '106.51.200.173'
SystemMacAddress = '30:05:05:CC:3A:89'