
import logging
from kiteconnect import KiteConnect
from Directories import *

with open(KiteEkanshLogin,'r') as a:
        content = a.readlines()
        a.close()
api_key = content[2].strip('\n')
kite = KiteConnect(api_key=api_key)
#Function will place order on the broker terminal, will take the necessary validation values from the text file
def order(order_details_fetch):#Tradetype,Exchange,Tradingsymbol,Quantity,Variety,Ordertype,Product,Validity,Price):
    
    Tradetype = order_details_fetch['Tradetype']
    Exchange = order_details_fetch['Exchange']
    Tradingsymbol = str(order_details_fetch['Tradingsymbol']).replace(" ","")
    Quantity = order_details_fetch['Quantity']
    Variety = order_details_fetch['Variety']
    Ordertype = order_details_fetch['Ordertype']
    Product = order_details_fetch['Product']
    Validity = order_details_fetch['Validity']
    Price = order_details_fetch['Price'] or 0.0
    OrderTag = str(order_details_fetch.get("OrderTag"))
    #print(order_details_fetch)  



    with open(KiteEkanshLoginAccessToken,'r') as f:
        access_tok = f.read()
        f.close()


    kite.set_access_token(access_tok)

    # Place an order
    
    dict = {"MARKET":kite.ORDER_TYPE_MARKET,"LIMIT":kite.ORDER_TYPE_LIMIT,"NSE":kite.EXCHANGE_NSE,"NFO":kite.EXCHANGE_NFO,"MCX":"MCX","CDS":kite.EXCHANGE_CDS,
            "buy":kite.TRANSACTION_TYPE_BUY,"sell":kite.TRANSACTION_TYPE_SELL,"AMO":kite.VARIETY_AMO,"REGULAR":kite.VARIETY_REGULAR,"NRML":kite.PRODUCT_NRML,"MIS":kite.PRODUCT_MIS,
            "CNC":kite.PRODUCT_CNC,"DAY":kite.VALIDITY_DAY,"IOC":kite.VALIDITY_IOC,"BO":kite.VARIETY_CO,"CO":kite.VARIETY_CO,"SL":kite.ORDER_TYPE_SL,"SLM":kite.ORDER_TYPE_SLM,
            "market":kite.ORDER_TYPE_MARKET,"limit":kite.ORDER_TYPE_LIMIT,"nse":kite.EXCHANGE_NSE,"nfo":kite.EXCHANGE_NFO,"mcx":"MCX","cds":kite.EXCHANGE_CDS,
            "BUY":kite.TRANSACTION_TYPE_BUY,"SELL":kite.TRANSACTION_TYPE_SELL,"amo":kite.VARIETY_AMO,"regular":kite.VARIETY_REGULAR,"nrml":kite.PRODUCT_NRML,"mis":kite.PRODUCT_MIS,
            "cnc":kite.PRODUCT_CNC,"day":kite.VALIDITY_DAY,"ioc":kite.VALIDITY_IOC,"bo":kite.VARIETY_CO,"co":kite.VARIETY_CO,"sl":kite.ORDER_TYPE_SL,"slm":kite.ORDER_TYPE_SLM            
             }
 
    try:
        order_id = kite.place_order(tradingsymbol=Tradingsymbol,
                                    exchange=dict[Exchange],
                                    transaction_type=dict[Tradetype],
                                    quantity=Quantity,
                                    variety=dict[Variety],
                                    order_type=dict[Ordertype],
                                    product=dict[Product],
                                    validity=dict[Validity],
                                    price=(Price or 0),
                                    tag = OrderTag)

        print('Order Placed for contract-->' + str(order_id))
        
        #logging.info("Order placed. ID is: {}".format(order_id))
    except Exception as e:
        logging.basicConfig(level=logging.DEBUG)
        logging.info("Order placement failed: {}".format(e))
        exit(1)


#def order_angel(Broker,Tradetype,Exchange,Tradingsymbol,Quantity,Variety,Ordertype,Product,Validity,Price):


if __name__ == '__main__':
    order()
    # Fetch all orders
    kite.orders()

    # Get instruments
    kite.instruments()

