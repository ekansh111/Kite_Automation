import logging
from Server_Order_Handler import EstablishConnectionAngelAPI
from SmartApi import SmartConnect

def fetchOrderBook(smartApi):
    """
    Fetches the order book from Angel One (SmartAPI) and returns the 'data' portion of the response.
    
    Returns:
        list: A list of order dictionaries (each representing an order).
    """
    logging.info("Fetching order book from SmartAPI...")
    orderBook = smartApi.orderBook()
    #print("Raw orderBook response:", orderBook)
    
    # The orders are typically in orderBook["data"] if 'orderBook' is a dict
    orderData = orderBook.get("data", [])
    return orderData

def convertOpenOrdersToMarket(smartApi):
    """
    Fetch all open orders from Angel One and convert them to MARKET orders.
    
    Parameters:
        smartApi (SmartConnect): The authenticated SmartConnect instance.
    """
    try:
        orderData = fetchOrderBook(smartApi)
        if not orderData:
            print("No orders in the order book.")
            return

        for order in orderData:
            orderId = order.get("orderid")
            variety = order.get("variety")
            status = order.get("status", "").lower()

            # Build a dictionary of order details
            OrderDetails = {
                "Variety":       order["variety"],
                "OrderId":       order["orderid"],
                "Tradingsymbol": order["tradingsymbol"],
                "Symboltoken":   order["symboltoken"],
                "Tradetype":     order["transactiontype"],  # e.g. "BUY" or "SELL"
                "Exchange":      order["exchange"],         # e.g. "NSE" or "MCX"
                "Ordertype":     order["ordertype"],        # e.g. "MARKET" or "LIMIT"
                "Product":       order["producttype"],      # e.g. "CARRYFORWARD" or "INTRADAY"
                "Validity":      order["duration"],         # e.g. "DAY", "IOC"
                "Quantity":      order["quantity"],         # could be str or int
                "Price":         order["price"],            # 0 if MARKET, else limit price
            }

            # Convert order to MARKET if status is open/pending/AMO
            # (The exact statuses may vary depending on the broker)
            if status in ["open", "pending", "after market order req received"]:
                logging.info(f"Converting order {orderId} to MARKET...")
                
                ModifyOrderParams = {
                    "variety":         OrderDetails["Variety"],
                    "orderid":         OrderDetails["OrderId"],
                    "tradingsymbol":   OrderDetails["Tradingsymbol"],
                    "symboltoken":     OrderDetails["Symboltoken"],
                    "transactiontype": OrderDetails["Tradetype"],
                    "exchange":        OrderDetails["Exchange"],
                    "ordertype":       "MARKET",   # Force to MARKET
                    "producttype":     OrderDetails["Product"],
                    "duration":        OrderDetails["Validity"],
                    "quantity":        OrderDetails["Quantity"],
                    "price":           "0",  # Usually 0 for MARKET
                }
                
                try:
                    # Note: Some versions require `smartApi.modifyOrder(**ModifyOrderParams)`
                    # (unpacking the dictionary) instead of passing the dict directly.
                    response = smartApi.modifyOrder(ModifyOrderParams)
                    print(f"Order {orderId} converted to MARKET. Response: {response}")
                except Exception as ex:
                    logging.error(f"Failed to modify order {orderId} to MARKET: {ex}")

    except Exception as e:
        logging.error(f"Error fetching or modifying orders: {e}")
        raise

def main():
    logging.basicConfig(level=logging.INFO)
    
    OrderUserDetails = {"User": "AABM826021"}
    # 1. Connect to Angel One
    smartApi = EstablishConnectionAngelAPI(OrderUserDetails)
    
    try:
        # 2. Convert open orders to market
        convertOpenOrdersToMarket(smartApi)
    finally:
        # End the session if desired
        try:
            smartApi.terminateSession(OrderUserDetails['User'])
        except Exception as ex:
            logging.warning(f"Error terminating session: {ex}")

if __name__ == "__main__":
    main()
