from kiteconnect import KiteConnect
import pandas as pd
from Directories import *
from datetime import datetime
from Server_Order_Place import order
import time


def fetch_positions_to_dataframe():
    try:
        # Fetch positions
        positions = kite.positions()
        
        # Extract net positions
        net_positions = positions['net']
        
        # Check if there are any positions
        if not net_positions:
            print("No positions found.")
            return pd.DataFrame()  # Return an empty DataFrame
        
        # Convert net positions to a DataFrame
        df_positions = pd.DataFrame(net_positions)
        
        # Display the DataFrame
        print("Positions Data:")
        print(df_positions)
        
        return df_positions

    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of an error

def place_orders(df_positions, kite, OrderType):
    """
    Place limit sell orders for positions of type MIS.
    
    Args:
        df_positions (pd.DataFrame): DataFrame containing positions.
    """
    ListOfOrderId = []
    try:
        # Filter positions of type MIS
        mis_positions = df_positions[df_positions['product'] == 'MIS']
        
        if mis_positions.empty:
            print("No MIS positions to place orders for.")
            return
        
        for index, position in mis_positions.iterrows():
            process_order(position, kite, OrderType, ListOfOrderId)

    except Exception as e:
        print(f"An error occurred while placing orders: {e}")
    return ListOfOrderId

def process_order(position, kite, OrderType, ListOfOrderId):
    """
    Process a single order based on the given position.

    Args:
        position (pd.Series): A pandas Series containing position details.
        kite: The KiteConnect object.
        OrderType (str): The type of order (e.g., 'LIMIT', 'MARKET').
        ListOfOrderId (list): A list to append the generated Order IDs.

    Returns:
        None
    """
    tradingsymbol = position['tradingsymbol']
    last_price = position['last_price']
    current_quantity = position['quantity']
    quantity = abs(current_quantity)  # Ensure positive quantity

    if quantity == 0:
        print('Quantity is 0, Skipping Symbol/Instrument ' + str(tradingsymbol))
        return

    product = 'MIS'  # 'CNC'#
    variety = 'REGULAR'  # 'AMO'#
    if current_quantity > 0:
        tradetype = 'SELL'  # 'SELL'
    else:
        tradetype = 'BUY'  # 'SELL'

    ordertype = OrderType

    # Set a limit price (for simplicity, we use the last traded price)
    limit_price = last_price

    print(f"Placing limit {tradetype.lower()} order for {tradingsymbol}: Quantity {quantity}, Limit Price {limit_price}")
    print(kite)

    OrderDetails = {
        'Tradetype': tradetype,
        'Exchange': position['exchange'],
        'Tradingsymbol': tradingsymbol,
        'Quantity': quantity,
        'Variety': variety,
        'Ordertype': ordertype,
        'Product': product,
        'Validity': 'DAY',
        'Price': limit_price,
        'Symboltoken': '',
        'Squareoff': '',
        'Stoploss': '',
        'Broker': '',
        'Netposition': '',
        'OptionExpiryDay': '',
        'OptionContractStrikeFromATMPercent': '',
        'Trigger': '',
        'StopLossTriggerPercent': '',
        'StopLossOrderPlacePercent': '',
        'CallStrikeRequired': '',
        'PutStrikeRequired': '',
        'Hedge': '',
        'OrderTag': '',
        'User': '',
        'TimePeriod': ''
    }

    OrderId = order(OrderDetails)
    ListOfOrderId.append(OrderId)
    print(f"Order ID is {OrderId} for {tradingsymbol}: Quantity {quantity}, Limit Price {limit_price}")

    # Optionally, you can check the order status here
    # order_history = kite.order_history(order_id=OrderId)
    # latest_status = order_history[-1]['status']
    # status_message = order_history[-1]['status_message']
    # print(f"Latest status of order {OrderId}: {latest_status}")
    # print(f"Status message: {status_message}")

    # exit(1)  # If you want to exit after placing one order (probably not needed)


def convert_limit_order_to_market(kite, order_id):
    """
    Convert an existing limit order to a market order.
    If direct modification is not possible, cancel the limit order and place a new market order.
    
    Parameters:
    - kite: An instance of the KiteConnect class with a valid access token.
    - order_id (str): The order ID of the existing limit order.
    - tradingsymbol (str): The trading symbol of the instrument (e.g., 'INFY').
    - exchange (str): The exchange on which the order is placed (e.g., kite.EXCHANGE_NSE).
    - transaction_type (str): kite.TRANSACTION_TYPE_BUY or kite.TRANSACTION_TYPE_SELL.
    - quantity (int): The quantity for the order.
    - product (str): The product code (e.g., kite.PRODUCT_CNC, kite.PRODUCT_MIS).
    
    Returns:
    - new_order_id (str): The order ID of the modified or newly placed market order.
    """
    try:
        # Attempt to modify the existing limit order to a market order
        kite.modify_order(
            variety=kite.VARIETY_REGULAR,
            order_id=order_id,
            order_type=kite.ORDER_TYPE_MARKET
            # Other parameters can be added if needed
        )
        print(f"Order {order_id} has been modified to a market order.")
        return order_id  # Returning the same order ID since it was modified
    except Exception as e:
        print(f"Failed to modify order {order_id}: {e}")
        
def get_order_status(kite, order_list, OrderType, ReorderFlag):
    """
    Fetches the latest status and status message of an order using its order_id.

    Parameters:
    - kite: An instance of the KiteConnect class with a valid access token.
    - order_id: The order ID of the order you want to check.

    Returns:
    - A tuple containing (latest_status, status_message).
      Returns (None, None) if the order history is not found or an exception occurs.
    """

    for order_id in order_list:
        try:
            # Fetch the order history for the specific order ID
            order_history = kite.order_history(order_id=order_id)
            
            if order_history:
                # The last entry in the order history contains the latest status
                latest_status = order_history[-1]['status']
                status_message = order_history[-1]['status_message']
                print(f"Latest status of order {order_id}: {latest_status}")
                print(f"Status message: {status_message}")
                if ((ReorderFlag == 1) and str(latest_status) == 'OPEN'):
                    convert_limit_order_to_market(kite,order_id)    
                #return latest_status, status_message
            else:
                print(f"No history found for order ID: {order_id}")
                return None, None

        except Exception as e:
            print(f"An error occurred: {e}")
            return None, None


# Fetch positions and save them to a DataFrame
if __name__ == "__main__":

    # Reading API key from a file
    with open(KiteEkanshLogin, 'r') as a:
        content = a.readlines()
    api_key = content[2].strip('\n')
    print(api_key)

    # Initialize Kite Connect
    kite = KiteConnect(api_key=api_key)

    # Reading access token from a file
    with open(KiteEkanshLoginAccessToken, 'r') as f:
        access_tok = f.read()

    kite.set_access_token(access_tok)

    positions_df = fetch_positions_to_dataframe()
    
    # Specify the output directory
    output_directory = r"C:\Users\ekans\OneDrive\Documents\Trading\PositionsData"
    
    # Generate the filename with date and time
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  # Format: YYYYMMDD_HHMMSS
    output_file = f"{output_directory}\positions_{timestamp}.csv"
    
    # Save the DataFrame to a CSV file
    if not positions_df.empty:
        positions_df.to_csv(output_file, index=False)
        print(f"Positions saved to '{output_file}'.")
        
        OrderType = 'LIMIT'
        # Place limit sell orders for MIS positions
        ListOfOrderId = place_orders(positions_df, kite, OrderType)

        time.sleep(120)
        OrderType = 'MARKET'
        get_order_status(kite, ListOfOrderId, OrderType, ReorderFlag=1)
    else:
        print("No data to save.")
