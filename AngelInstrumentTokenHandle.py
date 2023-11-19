from Login_Auto3_Angel import *
from FetchOptionContractName import *
import pandas as pd


#Function to fetch the Symbol token for any provided symbol
def FetchAngelInstrumentSymbolToken(order_details_fetch):
    InstrumentName = order_details_fetch['Tradingsymbol']
    #Fetch the consolidated list of symboltoken,tradingsymbol.. data from the file which should be updated periodically
    df = pd.read_csv('AngelInstrumentDetails.txt', dtype={"token": "string", "symbol": "string","name": "string", "expiry": "string","strike": int, "lotsize": int,"instrumenttype": "string", "exch_seg": "string", "tick_size": int})
    AngelInstrumentSymbolToken = df.loc[df['symbol'] == InstrumentName]['token'].to_string(index=False, header=False)
    #print('Instrument name-->' + str(AngelInstrumentSymbolToken))
    
    return AngelInstrumentSymbolToken

if __name__ == '__main__':
    #    m = {'Tradetype': 'BUY', 'Exchange': 'NFO', 'Tradingsymbol': 'NIFTY23NOV2319000CE', 'Quantity': '50', 'Variety': 'AMO', 'Ordertype': 'LIMIT', 'Product': 'CARRYFORWARD', 'Validity': 'DAY', 'Price': '335', 'Symboltoken':'48210', 'Squareoff':'', 'Stoploss':''}

    OrderDetails = {'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'NIFTY', 'Quantity': '50', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0.0,
            'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ANGEL','Netposition':'','OptionExpiryDay':'3','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'102',
            'StopLossOrderPlacePercent':'150','CallStrikeRequired':'True','PutStrikeRequired':'True','Hedge':'False',"OrderTag":"1NF-STR-MO-12-100"}

    #HandleOrderProcess(m)
    FetchAngelInstrumentSymbolToken(OrderDetails)
    #k =  FetchOptionName(OrderDetails)
    #print(k)