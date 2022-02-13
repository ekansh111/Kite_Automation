import logging
from kiteconnect import KiteConnect


with open('C:/Users/ekans/Documents/inputs/api_key.txt','r') as a:
        api_key = a.read()
        a.close()
kite = KiteConnect(api_key=api_key)
def order():
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

    try:
        order_id = kite.place_order(tradingsymbol="INFY",
                                    exchange=kite.EXCHANGE_NSE,
                                    transaction_type=kite.TRANSACTION_TYPE_BUY,
                                    quantity=1,
                                    variety=kite.VARIETY_AMO,
                                    order_type=kite.ORDER_TYPE_MARKET,
                                    product=kite.PRODUCT_CNC,
                                    validity=kite.VALIDITY_DAY)

        logging.info("Order placed. ID is: {}".format(order_id))
    except Exception as e:
        logging.info("Order placement failed: {}".format(e.message))


if __name__ == '__main__':
    order()
    # Fetch all orders
    kite.orders()

    # Get instruments
    kite.instruments()

