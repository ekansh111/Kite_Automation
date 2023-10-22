
from kiteconnect import KiteConnect
from datetime import datetime,timedelta
from datetime import date
import pandas as pd
option_sl = 0

with open('C:/Users/ekans/OneDrive/Documents/inputs/api_key_IK.txt','r') as a:
        api_key = a.read()
        a.close()
kite = KiteConnect(api_key=api_key)


with open('C:/Users/ekans/OneDrive/Documents/inputs/access_token_IK.txt','r') as f:
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



def Set_Gtt(ATM_VAL,Quantity,Trigger,StopLossTriggerPercent,StopLossOrderPlacePercent,Hedge):
    print(Hedge)
    if Hedge == 'False':
        print('hi')
        #Trigger should be greater or equal to 0 as the least trigger value is 0
        if Trigger >= 0:
            Trigger = Trigger - 1
        fetch_ltp = kite.ltp('NFO:' + ATM_VAL)
        option_ltp = int(fetch_ltp['NFO:'+ATM_VAL]['last_price'])
        print(option_ltp)

        option_trigger = (round((option_ltp*((100 + StopLossTriggerPercent)/100))*2,1)/2)#Multiplying by 2 to probably make rounding off easier
        option_sl = (round((option_ltp*((100 +StopLossOrderPlacePercent)/100))*2,1)/2)#set a slightly high sl value , since the order type sent is limit, so to 
                                                                                    #avoid a chance where the gtt is not triggered if the option values go past limit
                                                            
        sell_call = kite.place_gtt(trigger_type=gtt_trigger_type,
                                                        tradingsymbol=ATM_VAL,
                                                        exchange=order_exchange,
                                                        trigger_values=[option_trigger],
                                                        last_price=option_ltp,
                                                        orders=[{"transaction_type":order_buy,"quantity":Quantity,"price":option_sl,"order_type": order_type,"product": order_product}])
    elif Hedge == 'True':
        option_trigger = 'Hedge'
    #if str(ATM_VAL)[0:1] in {"B","b"}:
    data = [[ATM_VAL,option_trigger,Quantity,Trigger]]
    with open('C:/Users/ekans/Documents/inputs/option_details.csv', 'a', newline='') as newline:
        newline.close

    df = pd.DataFrame(data, columns=['OptionName','SL','Quantity','TriggerLeft'])                                                 
    df.to_csv('C:/Users/ekans/Documents/inputs/option_details.csv',header=True,index=False,mode='a')
    