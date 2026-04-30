import datetime
import os
from pathlib import Path
import datetime


icloudRoot= Path.home()
workInputRoot=icloudRoot/"Documents"/"Work"/"inputs"

KiteEkanshLogin = workInputRoot/'Login_Credentials.txt'
KiteEkanshLoginAPIKey = workInputRoot/'api_key_IK.txt'
KiteEkanshLoginAccessToken = workInputRoot/'access_token_IK.txt'
ZerodhaInstrumentDirectory = "ZerodhaInstruments.csv"

KiteRashmiLogin = workInputRoot/'Login_Credentials_YD6016.txt'
KiteRashmiLoginAccessToken = workInputRoot/'access_token_YD.txt'

KiteEshitaLogin = workInputRoot/'Login_Credentials_OFS653.txt'
KiteEshitaLoginAccessToken = workInputRoot/'access_token_OF.txt'

AngelEkanshLoginCred = workInputRoot/'Login_Credentials_Angel.txt'
AngelNararushLoginCred = workInputRoot/'Login_Credentials_Angel_Dad.txt'
AngelEshitaLoginCred = workInputRoot/'Login_Credentials_Angel_Eshita.txt'
AngelInstrumentDirectory = 'AngelInstrumentDetails.csv'

WriteOptionDetailsFile = workInputRoot/'option_details.csv'
WriteAllContractDet = workInputRoot/'TEXT_INSTRUMENTS.csv'

WorkDirectory = workInputRoot

workTradingRoot=icloudRoot/"Documents"/"Work"/"Trading"


IntraDayDirectory=str(workTradingRoot/"IntraDay_Stocks_Selector")
IntraDayDirectoryHistory=str(workTradingRoot/"IntraDay_Stocks_Selector"/"History4")
Nifty500ConstituentList=str(workTradingRoot/"IntraDay_Stocks_Selector"/"ind_nifty500list.csv")
PositionDataOpDirectory=str(workTradingRoot/"PositionsData")
Nifty500MACDDailyData="Nifty500DailyMACDData.csv"

MeanReversionCharts=str(workTradingRoot/"Scripts"/"Charts")
MeanReversionPortfolioValue=str(workTradingRoot/"Scripts"/"Charts"/"Portfolio Value")
MeanReversionZScore=str(workTradingRoot/"Scripts"/"Charts"/"z score")
MeanReversionSpreadResiduals=str(workTradingRoot/"Scripts"/"Charts"/"Spread and Residuals")
MeanReversionClosingPrice=str(workTradingRoot/"Scripts"/"Charts"/"Closing Prices")
MeanReversionCointigrationHeatMap=str(workTradingRoot/"Scripts"/"Charts"/"Cointegration Heatmaps")

# Get the current date
today=datetime.date.today()
month_str=today.strftime("%b").upper()
year_str=today.strftime("%Y")
month_year_dir=f"{month_str}{year_str}"

momentumRoot=workTradingRoot/"Momentum Stock Investing Data"/month_year_dir
RelativeMomentumDirectoryReturns=str(momentumRoot/"relative momentum returns.csv")
AbsoluteMomentumOutputDirectory=str(momentumRoot/"absolute momentum returns.csv")
MomentumFilesOutputDirectory=str(momentumRoot)
Nifty500ConstituentFilePath=str(momentumRoot/"ind_nifty500list.csv")

DEFAULT_SYMBOLS_FILE_INT50=str(workTradingRoot/"ConstituentDetails"/"ind_nifty200list.csv")
DEFAULT_OUTPUT_DIR_INT50=str(workTradingRoot/"IntradayNifty50")

# Email notification config. Password lives in local_secrets.py (gitignored)
# so it never lands in source/git. Falls back to KITE_EMAIL_PASSWORD env var
# if local_secrets.py is missing (e.g., fresh clone before setup).
EMAIL_NOTIFY_ENABLED = True
EMAIL_FROM = "ekansh.n111@gmail.com"
try:
    from local_secrets import EMAIL_FROM_PASSWORD
except ImportError:
    EMAIL_FROM_PASSWORD = os.environ.get("KITE_EMAIL_PASSWORD", "")
EMAIL_TO = "ekansh.n@gmail.com"
EMAIL_SMTP = "smtp.gmail.com"
EMAIL_PORT = 465
