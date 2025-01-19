import csv
import logging
from kiteconnect import KiteConnect
from datetime import datetime
import time
import pandas as pd

# Example directory constants—adjust as needed
from Directories import KiteEkanshLogin, ZerodhaInstrumentDirectory

def download_instruments_kite():
    """
    Connects to the Kite API using credentials from a local file,
    fetches the entire instrument list, and writes it to a CSV file.
    
    Adds light "garbage" checks to skip records that are missing critical fields
    like 'tradingsymbol' or 'instrument_token'.
    """
    # ----------------------------------------------------------------------
    # 1. Read Credentials and Initialize KiteConnect
    # ----------------------------------------------------------------------
    with open(KiteEkanshLogin, 'r') as cred_file:
        content = cred_file.readlines()
        cred_file.close()
    
    # In this example, api_key is assumed to be on line 2. 
    # Adjust indexing if your credentials file layout differs.
    api_key = content[2].strip('\n')
    
    # Initialize KiteConnect with the api_key
    kite = KiteConnect(api_key=api_key)
    
    # ----------------------------------------------------------------------
    # 2. Retrieve Instruments
    # ----------------------------------------------------------------------
    try:
        instruments = kite.instruments()
    except Exception as e:
        logging.error(f"Error fetching instruments from Kite: {e}")
        return
    
    # ----------------------------------------------------------------------
    # 3. Prepare CSV for Writing
    # ----------------------------------------------------------------------
    with open(ZerodhaInstrumentDirectory, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        
        # Define header consistent with the order of data you’ll write
        header = [
            'token',           # record['instrument_token']
            'symbol',          # record['tradingsymbol']
            'name',            # record['name']
            'expiry',          # record['expiry']
            'strike',          # record['strike']
            'lotsize',         # record['lot_size']
            'instrumenttype',  # record['instrument_type']
            'exch_seg',        # record['exchange']
            'tick_size',       # record['tick_size']
            'segment',         # record['segment']
            'exchange_token',  # record['exchange_token']
            'last_price'       # record['last_price']
        ]
        
        # Write header row
        csvwriter.writerow(header)
        
        # ------------------------------------------------------------------
        # 4. Write Cleaned Rows
        # ------------------------------------------------------------------
        record_count = 0
        for record in instruments:
            
            # Skip or "clean" out any record missing essential fields.
            # Adjust checks if your logic differs about "garbage" values.
            if (not record.get('instrument_token') or 
                not record.get('tradingsymbol')):
                continue  # skip this record

            # Create row_values from valid fields, handling possible None
            # by converting them to empty strings or skipping them as needed.
            row_values = [
                record['instrument_token'] if record.get('instrument_token') else '',
                record['tradingsymbol']     if record.get('tradingsymbol')     else '',
                record['name']              if record.get('name')              else '',
                record['expiry']            if record.get('expiry')            else '',
                record['strike']            if record.get('strike')            else '',
                record['lot_size']          if record.get('lot_size')          else '',
                record['instrument_type']   if record.get('instrument_type')   else '',
                record['exchange']          if record.get('exchange')          else '',
                record['tick_size']         if record.get('tick_size')         else '',
                record['segment']           if record.get('segment')           else '',
                record['exchange_token']    if record.get('exchange_token')    else '',
                record['last_price']        if record.get('last_price')        else ''
            ]
            
            csvwriter.writerow(row_values)
            record_count += 1
        
        logging.info(f"Successfully wrote {record_count} records to {ZerodhaInstrumentDirectory}.")

# ----------------------------------------------------------------------
# 5. Optionally call the function if running as a script
# ----------------------------------------------------------------------
if __name__ == "__main__":
    download_instruments_kite()
