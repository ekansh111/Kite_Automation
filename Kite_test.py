from ast import Constant
from codecs import decode
from email import message
import logging
from kiteconnect import KiteConnect


with open('C:/Users/ekans/Documents/inputs/api_key.txt','r') as a:
        api_key = a.read()
        a.close()
kite = KiteConnect(api_key=api_key)
def order(Tradetype,Exchange,Tradingsymbol,Quantity,Variety,Ordertype,Product,Validity):
    logging.basicConfig(level=logging.DEBUG)



    with open('C:/Users/ekans/Documents/inputs/access_token.txt','r') as f:
        access_tok = f.read()
        f.close()

    

    # Redirect the user to the login url obtained
    # from kite.login_url(), and receive the request_token
    # from the registered redirect url after the login flow.
    # Once you have the request_token, obtain the access_token
    # as follows.
    print("login here:",kite.login_url())

    kite.set_access_token(access_tok)

    # Place an order
    
    dict = {"MARKET":kite.ORDER_TYPE_MARKET,"LIMIT":kite.ORDER_TYPE_LIMIT,"NSE":kite.EXCHANGE_NSE,"NFO":kite.EXCHANGE_NFO,"MCX":kite.EXCHANGE_MCX,"CDS":kite.EXCHANGE_CDS,
            "buy":kite.TRANSACTION_TYPE_BUY,"sell":kite.TRANSACTION_TYPE_SELL,"AMO":kite.VARIETY_AMO,"REGULAR":kite.VARIETY_REGULAR,"NRML":kite.PRODUCT_NRML,"MIS":kite.PRODUCT_MIS,
            "CNC":kite.PRODUCT_CNC,"DAY":kite.VALIDITY_DAY,"IOC":kite.VALIDITY_IOC,"BO":kite.VARIETY_BO,"CO":kite.VARIETY_CO,"SL":kite.ORDER_TYPE_SL,"SLM":kite.ORDER_TYPE_SLM,
            "market":kite.ORDER_TYPE_MARKET,"limit":kite.ORDER_TYPE_LIMIT,"nse":kite.EXCHANGE_NSE,"nfo":kite.EXCHANGE_NFO,"mcx":kite.EXCHANGE_MCX,"cds":kite.EXCHANGE_CDS,
            "BUY":kite.TRANSACTION_TYPE_BUY,"SELL":kite.TRANSACTION_TYPE_SELL,"amo":kite.VARIETY_AMO,"regular":kite.VARIETY_REGULAR,"nrml":kite.PRODUCT_NRML,"mis":kite.PRODUCT_MIS,
            "cnc":kite.PRODUCT_CNC,"day":kite.VALIDITY_DAY,"ioc":kite.VALIDITY_IOC,"bo":kite.VARIETY_BO,"co":kite.VARIETY_CO,"sl":kite.ORDER_TYPE_SL,"slm":kite.ORDER_TYPE_SLM            
             }
 
    try:
        order_id = kite.place_order(tradingsymbol=Tradingsymbol,
                                    exchange=dict[Exchange],
                                    transaction_type=dict[Tradetype],
                                    quantity=Quantity,
                                    variety=dict[Variety],
                                    order_type=dict[Ordertype],
                                    product=dict[Product],
                                    validity=dict[Validity]
                                    )

        logging.info("Order placed. ID is: {}".format(order_id))
    except Exception as e:
        logging.info("Order placement failed: {}".format(e.message))


if __name__ == '__main__':
    order()
    # Fetch all orders
    kite.orders()

    # Get instruments
    kite.instruments()

