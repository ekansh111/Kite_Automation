
from kiteconnect import KiteConnect
from datetime import datetime,timedelta
from datetime import date
from Login_Auto3_Angel import Login_Angel_Api
import pandas as pd
from Directories import *
import csv
option_sl = 0

#with open(KiteEkanshLoginAPIKey,'r') as a:
#        api_key = a.read()
#        a.close()


#Fetch input values from the file
with open(KiteEkanshLogin,'r') as a:
        content = a.readlines()
        a.close()

user_id= content[0].strip('\n')
user_pwd = content[1].strip('\n')
api_key = content[2].strip('\n')
api_secret = content[3].strip('\n')
totp_key= content[4].strip('\n')

kite = KiteConnect(api_key=api_key)

with open(KiteEkanshLoginAccessToken,'r') as f:
    access_tok = f.read()
    f.close()
    #print(access_tok)
kite.set_access_token(access_tok)

gtt_trigger_type = kite.GTT_TYPE_SINGLE

order_type = kite.ORDER_TYPE_LIMIT

order_exchange = kite.EXCHANGE_NFO

order_variety = kite.VARIETY_REGULAR

order_product = kite.PRODUCT_NRML

order_buy = kite.TRANSACTION_TYPE_BUY

order_sell = kite.TRANSACTION_TYPE_SELL

order_validity = kite.VALIDITY_DAY  



def Set_Gtt(OrderDetails):
    #print('setgtt')
    print(OrderDetails)
    ATM_VAL = OrderDetails['Tradingsymbol']
    Quantity = OrderDetails['Quantity']
    Trigger = int(OrderDetails['Trigger'])
    StopLossTriggerPercent = int(OrderDetails['StopLossTriggerPercent'])
    StopLossOrderPlacePercent = int(OrderDetails['StopLossOrderPlacePercent'])
    Hedge = OrderDetails['Hedge']

    #print(Hedge)
    if Hedge == 'False':
        #Trigger should be greater or equal to 0 as the least trigger value is 0
        if Trigger >= 0:
            Trigger = Trigger - 1
        
        #If the order needs to be placed Angel then route through a different process as Instrument names are different
        if OrderDetails.get("Broker") == 'ANGEL':
            smartApi = Login_Angel_Api(OrderDetails)
            fetch_ltp = smartApi.ltpData(exchange= 'NFO',tradingsymbol=ATM_VAL,symboltoken=OrderDetails['Symboltoken'])
            option_ltp = int(fetch_ltp['data']['ltp'])
        else:             
            fetch_ltp = kite.ltp('NFO:' + ATM_VAL)
            option_ltp = int(fetch_ltp['NFO:'+ATM_VAL]['last_price'])

        option_trigger = (round((option_ltp*((100 + StopLossTriggerPercent)/100))*2,1)/2)#Multiplying by 2 to probably make rounding off easier
        option_sl = (round((option_ltp*((100 +StopLossOrderPlacePercent)/100))*2,1)/2)#set a slightly high sl value , since the order type sent is limit, so to 
                                                                                    #avoid a chance where the gtt is not triggered if the option values go past limit
        #print(str(ATM_VAL) +'|'+ str(OrderDetails['Symboltoken']) +'|'+ str(option_sl) +'|'+ str(Quantity) +'|'+ str(option_trigger) +'|'+ str(OrderDetails['TimePeriod']))
        #If the order needs to be placed for angel broking account
        if OrderDetails.get("Broker") == 'ANGEL':
            gttCreateParams = {
                                "tradingsymbol": ATM_VAL,
                                "symboltoken": OrderDetails['Symboltoken'],
                                "exchange": 'NFO',
                                "producttype": "CARRYFORWARD",
                                "transactiontype": 'BUY',
                                "price": option_sl,
                                "qty": Quantity,
                                "triggerprice": option_trigger,
                                "timeperiod": OrderDetails['TimePeriod']
                            }
            smartApi.gttCreateRule(gttCreateParams)
        else:
            kite.place_gtt(trigger_type=gtt_trigger_type,
                                                        tradingsymbol=ATM_VAL,
                                                        exchange=order_exchange,
                                                        trigger_values=[option_trigger],
                                                        last_price=option_ltp,
                                                        orders=[{"transaction_type":order_buy,"quantity":Quantity,"price":option_sl,"order_type": order_type,"product": order_product}])
    elif Hedge == 'True':
        option_trigger = 'Hedge'
    elif Hedge == 'MonthlyCall':
        option_trigger = 'MonthlyCallBuy'
    write_order_details_to_csv(OrderDetails, WriteOptionDetailsFile)

def write_order_details_to_csv(OrderDetails, csv_file_path):
    # Extract keys and values from the dictionary
    keys = list(OrderDetails.keys())
    values = list(OrderDetails.values())

    # Get the current time
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Check if the CSV file exists, create it if not
    with open(csv_file_path, 'a', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)


        csvwriter.writerow(['Timestamp'] + keys)

        # Write values row with current time
        csvwriter.writerow([current_time] + values)

    