
from kiteconnect import KiteConnect
from datetime import datetime,timedelta
from datetime import date
option_sl = 0

with open('C:/Users/ekans/Documents/inputs/api_key_IK.txt','r') as a:
        api_key = a.read()
        a.close()
kite = KiteConnect(api_key=api_key)


with open('C:/Users/ekans/Documents/inputs/access_token_IK.txt','r') as f:
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



def Set_Gtt(ATM_VAL,Quantity):
    fetch_ltp = kite.ltp('NFO:' + ATM_VAL)
    option_ltp = int(fetch_ltp['NFO:'+ATM_VAL]['last_price'])
    if str(ATM_VAL)[0:1] in {"B","b"}:
        option_trigger = (option_ltp*(201/100))
        option_sl = (option_ltp*(241/100))

    if str(ATM_VAL)[0:1] in {"N","n"}:
        option_trigger = (option_ltp*(146/100))
        option_sl = (option_ltp*(176/100))

    sell_call = kite.place_gtt(trigger_type=gtt_trigger_type,
                                                    tradingsymbol=ATM_VAL,
                                                    exchange=order_exchange,
                                                    trigger_values=[option_trigger],
                                                    last_price=option_ltp,
                                                    orders=[{"transaction_type":order_buy,"quantity":Quantity,"price":option_sl,"order_type": order_type,"product": order_product}])
                                                                

                                                            