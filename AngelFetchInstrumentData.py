import pandas as pd
from Directories import *

import time
start_time = time.time()
df = pd.read_json('http://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json')
#df = pd.read_csv('test.txt', dtype={"token": "string", "symbol": "string","name": "string", "expiry": "string","strike": int, "lotsize": int,"instrumenttype": "string", "exch_seg": "string", "tick_size": int})
#print(df)
df.to_csv(AngelInstrumentDirectory)

print("--- %s seconds ---" % (time.time() - start_time))
