"""
Description:
This script demonstrates how to manage Good Till Triggered (GTT) orders using the Zerodha KiteConnect API.
It shows how to fetch all GTT orders, compare a given GTT ID against existing orders, and cancel a specific GTT order.
Additionally, the script sets up a KiteConnect session by reading necessary credentials from files.
"""

from kiteconnect import KiteConnect
from Directories import *
import pandas as pd

def get_all_gtt_orders(kite):
    """
    Fetches and prints all GTT (Good Till Triggered) orders from the Kite account.
    Assumes that kite._routes["gtt.triggers"] has been set to the appropriate endpoint.
    
    Returns:
        A list of GTT orders (each order is a dictionary).
    """
    # Fetch all GTT triggers
    response = kite._get("gtt.triggers")

    # Since response is a list, we can directly assign
    gtt_orders = response

    # Convert the list of GTT orders to a DataFrame
    gtt_orders_df = pd.DataFrame(gtt_orders)
        
    return gtt_orders_df

def compare_gtt_id(gtt_id, gtt_orders_df):
    """
    Checks if the given gtt_id exists in the gtt_orders DataFrame.
    
    Parameters:
        gtt_id: The GTT ID to check.
        gtt_orders_df: A pandas DataFrame containing GTT orders with an 'id' column.
    
    Returns:
        True if gtt_id is found in gtt_orders_df, False otherwise.
    """
    # Instead of using "in" on values, use .isin() which returns a boolean series,
    # then use .any() to see if any rows match.
    return gtt_orders_df["id"].isin([gtt_id]).any()

def cancel_gtt(kite: KiteConnect, gtt_id: int) -> dict:
    """
    Cancels the GTT order with the specified GTT ID.
    
    Parameters:
        kite (KiteConnect): An initialized KiteConnect instance with a valid access token.
        gtt_id (int): The ID of the GTT order to cancel.
        
    Returns:
        dict: The response dictionary from the Kite API on successful cancellation.
    
    Raises:
        Exception: If the API call fails due to network errors, authentication, or other issues.
    """
    try:
        response = kite.delete_gtt(gtt_id)
        print(f"GTT with ID {gtt_id} canceled successfully.")
        return response
    except Exception as e:
        print(f"Failed to cancel GTT with ID {gtt_id}. Error: {e}")
        raise

# Example usage:
# Assuming `kite` is a valid KiteConnect instance with an active session.
# gtt_id_to_cancel = 123456  # Replace with your actual GTT ID
# cancel_gtt(kite, gtt_id_to_cancel)

# --- Main code execution ---

# if __name__ == '__main__':

# Fetch input values from the file
with open(KiteEkanshLogin,'r') as a:
    content = a.readlines()

user_id= content[0].strip('\n')
user_pwd = content[1].strip('\n')
api_key = content[2].strip('\n')
api_secret = content[3].strip('\n')
totp_key= content[4].strip('\n')

kite = KiteConnect(api_key=api_key)

with open(KiteEkanshLoginAccessToken,'r') as f:
    access_tok = f.read()

kite.set_access_token(access_tok)

# Manually add the route if needed
kite._routes["gtt.triggers"] = "/gtt/triggers"

'''
# Get all GTT orders
gtt_orders_df = get_all_gtt_orders(kite)

# Example usage of compare_gtt_id function
test_gtt_id = 253271319  # Replace this with an actual GTT ID to test
exists = compare_gtt_id(test_gtt_id, gtt_orders_df)
print(f"Does GTT ID {test_gtt_id} exist? {exists}")

if exists:
    cancel_gtt(kite,test_gtt_id)
'''
