from IntraDay_Stocks_Place_Order import *  # Assumes convert_limit_order_to_market is defined here
from Directories import *                 # Assumes KiteEkanshLogin and KiteEkanshLoginAccessToken are defined here
from kiteconnect import KiteConnect     # Import KiteConnect client library

def connectToUserKiteApi(userId, loginPath, accessTokenPath):
    """
    Connect to the user's Kite API using credentials stored in files.

    Parameters:
      - userId (str): The user identifier.
      - loginPath (str): File path containing login details (API key expected on the 3rd line).
      - accessTokenPath (str): File path containing the access token.

    Returns:
      - kite: An instance of the KiteConnect class with the access token set.
    """
    try:
        # Read API key from the login file. 
        # Note: The API key is assumed to be on the 3rd line (index 2).
        with open(loginPath, 'r') as file:
            content = file.readlines()
        api_key = content[2].strip()
        # For debugging purposes only. Remove or mask this in production.
        print(f"API Key: {api_key}")

        # Initialize Kite Connect with the API key.
        kite = KiteConnect(api_key=api_key)

        # Read access token from its file.
        with open(accessTokenPath, 'r') as f:
            access_token = f.read().strip()

        # Set the access token so that subsequent API calls are authenticated.
        kite.set_access_token(access_token)

        return kite
    except Exception as e:
        print(f"Error connecting to Kite API: {e}")
        # Exit if connection fails. Alternatively, you might raise an exception.


def processOpenOrders(kite, symbolFilter=None):
    """
    Fetch open orders and return the order details in a dictionary (hash).

    Parameters:
      - kite: An instance of the KiteConnect class.
      - symbolFilter (str, optional): If provided, only orders for this trading symbol will be processed.

    Returns:
      - ordersData (dict): A dictionary with order IDs as keys. Each value is a dictionary containing:
          'orderId', 'tradingSymbol', 'orderType', 'exchange', 'transactionType', 'quantity', 'product', 'variety'
    """
    try:
        # Fetch all orders (typically for the current trading day)
        orders = kite.orders()
    except Exception as e:
        print(f"Error fetching orders: {e}")
        return {}

    # Filter orders with status 'OPEN'
    openOrders = [order for order in orders if order.get('status') in ['OPEN', 'OPEN PENDING']]
    if not openOrders:
        print("No open or pending orders found.")
        return {}

    ordersData = {}
    for order in openOrders:
        # Extract relevant order details.
        orderId = order.get('order_id')
        tradingSymbol = order.get('tradingsymbol')
        orderType = order.get('order_type').upper() if order.get('order_type') else ''
        exchange = order.get('exchange')
        transactionType = order.get('transaction_type')
        quantity = order.get('quantity')
        product = order.get('product')
        variety = order.get('variety', 'regular')

        # If a symbol filter is provided, skip orders that don't match.
        if symbolFilter and tradingSymbol != symbolFilter:
            continue

        # Build a dictionary (hash) for order details.
        orderDetails = {
            'orderId': orderId,
            'tradingSymbol': tradingSymbol,
            'orderType': orderType,
            'exchange': exchange,
            'transactionType': transactionType,
            'quantity': quantity,
            'product': product,
            'variety': variety
        }
        # Use the order ID as the key.
        ordersData[orderId] = orderDetails

        print(f"\nProcessing order {orderId} for symbol {tradingSymbol} with type {orderType}.")
        if orderType == kite.ORDER_TYPE_MARKET:
            print("Order is already a market order, skipping conversion.")

    return ordersData

def main():
    """
    Main function to connect to the Kite API, fetch open orders, and convert limit orders to market orders.
    """
    # Connect to the Kite API using credentials defined in your Directories module.
    kite = connectToUserKiteApi('IK6635', KiteEkanshLogin, KiteEkanshLoginAccessToken)
    
    # Fetch open orders. Pass a specific symbol as symbolFilter if needed (e.g., 'INFY').
    ordersData = processOpenOrders(kite, symbolFilter=None)
    
    # Iterate through each open order and attempt to convert limit orders to market orders.
    for orderId, orderDetails in ordersData.items():
        # Only attempt conversion if the order type is not already market.
        if orderDetails['orderType'] != kite.ORDER_TYPE_MARKET:
            convert_limit_order_to_market(kite, orderId)
    
    # Output the final orders data.
    print("\nFinal orders data:")
    print(ordersData)

if __name__ == "__main__":
    main()
