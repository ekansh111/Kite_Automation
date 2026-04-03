"""
Description:
This script demonstrates a workflow for placing and exiting FNO trades in Zerodha Kite using a week-based SL (stop-loss) strategy, 
and optionally setting GTT (Good Till Triggered) orders for exits. It also fetches historical GTT IDs based on stored CSV entries, 
and processes exit orders using the retrieved GTT details. The script supports multiple order tags, each with a distinct SL configuration 
per week of the month, including a special configuration after the last Thursday of the month.
"""
from PlaceFNOTradesKite import *
from FetchOptionContractName import FetchOptionName
from kiteconnect import KiteConnect
from Server_Order_Place import order
from Set_Gtt_Exit import Set_Gtt
from datetime import timedelta, date
from dateutil.relativedelta import relativedelta
from Holidays import CheckForDateHoliday
from Login_Auto3_Angel import *
from AngelInstrumentTokenHandle import *
from Directories import *
import calendar
import csv
from Fetch_GTT import *
import datetime

def set_week_based_sl(OrderDetails):
    """
    Sets the stop-loss (SL) value based on the week of the month the order is placed
    and whether the order is after the last Thursday of the month.

    The logic:
    - If the order date is on or after the last Thursday of the month, use a special SL value.
    - Otherwise, determine which week of the month it is (1st, 2nd, 3rd, 4th) and set SL accordingly.
    
    Different `OrderTag` can have different sets of SL values.
    """

    # Define SL values for different ordertags and weeks
    # Adjust these values as per your strategy
    # Example structure:
    # SL_CONFIG = {
    #     "OrderTag1": {"1": a, "2": b, "3": c, "4": d, "POST_LAST_THUR": post_last_thur_sl},
    #     "12FN-SC-MACD-WE-65": {"1": 100, "2": 110, "3": 120, "4": 130, "POST_LAST_THUR": 150},
    #     ...
    # }

    SL_CONFIG = {
        "21BN-STR-TH": {"1": 33, "2": 75, "3": 75, "4": 80, "POST_LAST_THUR": 30},
        "22MN-STR-TH": {"1": 33, "2": 75, "3": 75, "4": 80, "POST_LAST_THUR": 30},
        "25BN-SP-MD-TH": {"1": 33, "2": 75, "3": 75, "4": 80, "POST_LAST_THUR": 30},
        "25BN-SC-MD-TH": {"1": 33, "2": 75, "3": 75, "4": 80, "POST_LAST_THUR": 30},
        # Add other ordertags and their corresponding week-based SL values here
    }

    # Extract OrderTag from OrderDetails
    for OrderInfo in OrderDetails:
        continue

    order_tag = OrderDetails[OrderInfo].get("OrderTag", None)
    if not order_tag or order_tag not in SL_CONFIG:
        # If no OrderTag or not in config, return from function
        return

    # Determine the current date and find the last Thursday of the month
    now = datetime.datetime.now()
    year = now.year
    month = now.month

    # Find the last Thursday of this month
    last_day = calendar.monthrange(year, month)[1]
    last_thursday = None
    for d in range(last_day, 0, -1):
        dt = date(year, month, d)
        if dt.weekday() == 3:  # Thursday is 3 (Monday=0, Tuesday=1, ...)
            last_thursday = dt
            break

    today_date = now.date()

    # Check if today is on or after the last Thursday
    if today_date >= last_thursday:
        # Use the special SL for post-last-Thursday scenario
        sl_value = SL_CONFIG[order_tag]["POST_LAST_THUR"]
        week_str = 4
    else:
        # Determine the week number of the month
        # Week calculation: 
        # 1st week = days 1-7, 2nd = 8-14, 3rd = 15-21, 4th = 22-28 (or beyond if needed)
        day_of_month = today_date.day
        week_number = (day_of_month - 1) // 7 + 1  # integer division
        
        # Cap the week number to 4 if it goes beyond
        if week_number > 4:
            week_number = 4
        
        week_str = str(week_number)
        sl_value = SL_CONFIG[order_tag][week_str]

    # Set the SL value in OrderDetails
    for OrderType in OrderDetails:
        Tag = str(OrderDetails[OrderType]['OrderTag']) + 'WK' + str(week_str) + '-' + str(sl_value)

        OrderDetails[OrderType]['StopLossTriggerPercent'] = int(sl_value)
        OrderDetails[OrderType]['StopLossOrderPlacePercent'] = int(sl_value + ((sl_value*33)/100))
        OrderDetails[OrderType]['OrderTag'] = Tag
    print(f"SL set to {sl_value} for OrderTag {Tag}, placed on {now.strftime('%Y-%m-%d')}.")
    return OrderDetails

def Fetch_Historical_GTTId(OrderDetails, DateOfTrade, csv_file_path):
    """
    Fetch the GTT ID from the CSV file based on the specified OrderTag and DateOfTrade.
    
    Parameters:
        OrderTag (str): The order tag to search for.
        DateOfTrade (str): The date of trade in 'YYYY-MM-DD' format.
        csv_file_path (str): The path to the CSV file containing order logs.
    
    Returns:
        str or None: The GTT ID if found, otherwise None.
    """
    for orderinfo in OrderDetails:
        OrderTag = str(OrderDetails[orderinfo]['OrderTag'])

    with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
        csvreader = csv.reader(csvfile)
        gttid_value_list = []

        # Since each write operation writes 2 rows: one header row and one data row
        # We will read the file line by line in pairs.
        lines = list(csvreader)
        
        # Process in steps of 2 lines (header, values)
        for i in range(0, len(lines), 2):
            if i + 1 >= len(lines):
                # If we don't have a pair, break
                break
            
            header_line = lines[i]
            value_line = lines[i+1]
            
            # Ensure we have at least as many values as headers
            if len(value_line) < len(header_line):
                continue
            
            # Find indices of required columns
            # We know Timestamp is at index 0 because we always write ['Timestamp'] + keys
            timestamp_index = 0
            
            # Get indices for OrderTag and GTTId if they exist
            try:
                ordertag_index = header_line.index("OrderTag")
                gttid_index = header_line.index("GTTId")
            except ValueError:
                # If for some reason OrderTag or GTTId is not found in headers, skip
                continue

            # Extract values
            timestamp_str = str(datetime.datetime.strptime(str(value_line[timestamp_index]), "%Y-%m-%d %H:%M:%S"))                           
            ordertag_value = value_line[ordertag_index]
            gttid_value = value_line[gttid_index]

            #print(timestamp_str)
            # Check date and order tag conditions
            # Timestamp format as saved is 'YYYY-MM-DD HH:MM:SS', so we can slice the date
            #print(timestamp_str)
            if timestamp_str.startswith(DateOfTrade) and ordertag_value.startswith(OrderTag):
                gttid_value_list.append(gttid_value)
        print(gttid_value_list)
        return gttid_value_list

    # If no match found
    return None

def FetchHistoricalDateOrderPlaced(OrderDetails):
    for OrderInfo in OrderDetails:
        continue

    OrderTag = OrderDetails[OrderInfo]['OrderTag']
    today = datetime.datetime.now().date()
    
    # Calculate Monday of the current week (Monday=0)
    monday_of_current_week = today - timedelta(days=today.weekday())
    
    if OrderTag == "21BN-STR-TH":
        # Last Friday of the previous week
        # Friday has weekday=4 (Mon=0, Tue=1, Wed=2, Thu=3, Fri=4)
        # Previous week's Friday is 3 days before this week's Monday (Mon=0)
        HistoricalOrderPlacedDate = (monday_of_current_week - timedelta(days=3)).strftime('%Y-%m-%d')
    
    elif OrderTag == "22MN-STR-TH":
        # Last Wednesday of the previous week
        # Wednesday is weekday=2, so Wednesday of last week is Monday_of_current_week - 5 day
        HistoricalOrderPlacedDate = (monday_of_current_week - timedelta(days=5)).strftime('%Y-%m-%d')
    
    elif OrderTag == "25BN-SP-MD-TH":
        HistoricalOrderPlacedDate = (monday_of_current_week - timedelta(days=4)).strftime('%Y-%m-%d')

    elif OrderTag == "25BN-SC-MD-TH":
        HistoricalOrderPlacedDate = (monday_of_current_week - timedelta(days=4)).strftime('%Y-%m-%d')

    else:
        # If neither condition matches, you may decide what to return or raise an error
        HistoricalOrderPlacedDate = None

    return HistoricalOrderPlacedDate

def process_exit_orders(OrderDetails, kite, WriteOptionDetailsFile):
    HistoricalOrderDate = FetchHistoricalDateOrderPlaced(OrderDetails)
    print('Last week historical order placed date ' + str(HistoricalOrderDate))
    #HistoricalOrderDate = '2024-12-18'
    
    GTTId_To_Compare_List = Fetch_Historical_GTTId(OrderDetails, HistoricalOrderDate, WriteOptionDetailsFile)
    
    # Get all GTT orders
    gtt_orders_df = get_all_gtt_orders(kite)
    
    for GTTId_To_Compare in GTTId_To_Compare_List:
        # Conversion to int necessary for comparison
        exists = compare_gtt_id(int(GTTId_To_Compare), gtt_orders_df)
        print('Does the GTTId exist in the list ' + str(exists))
        if exists:
            GttPlacedDetails = pd.DataFrame(gtt_orders_df[gtt_orders_df['id'] == int(GTTId_To_Compare)])
            #print(GttPlacedDetails.iloc[0]['condition']['tradingsymbol'])

            for orderinfo in OrderDetails:
                OrderDetails[orderinfo]['Tradingsymbol'] = str(GttPlacedDetails.iloc[0]['condition']['tradingsymbol'])
                if str(OrderDetails[orderinfo]['Tradingsymbol'])[-2:] == 'CE':
                    OrderDetails[orderinfo]['CallStrikeRequired'] = 'True'
                if str(OrderDetails[orderinfo]['Tradingsymbol'])[-2:] == 'PE':
                    OrderDetails[orderinfo]['PutStrikeRequired'] = 'True'

            # Attempt to Cancel the GTT order safely
            try:
                cancel_gtt(kite, int(GTTId_To_Compare))
            except Exception as e:
                print(f"Skipping cancellation for GTT ID {GTTId_To_Compare}: {e}")

            # Place order for Zerodha kite terminal
            PlaceOrders(OrderDetails[orderinfo])
            print(OrderDetails[orderinfo])



if __name__ == '__main__':
    one_shot_flag = True
    print(" G--Go Ahead!  N-->Abort the execution  M-->Modify any of the parameters")
    try:
        proceed = inputimeout(timeout=5)
        if proceed in {"G","g"}:
            Override = False    
        if proceed in {"M","m"}:
            print(" 95--BankNiftyStraddle_Fri_945Am_Monthly \n 96--BankNiftyStraddle_Fri_945Am_Monthly_Exit  \n 97--MidCPNiftyStraddle_Wed_13Pm_Monthly \n 98--MidCPNiftyStraddle_Wed_13Pm_Monthly_Exit \n 99--BankNiftyMACD_Thu_15Pm_Monthly_Exit \n 100--MidCPNiftyMACD_Mon_15Pm_Monthly_Exit ")
            Override = input("Enter the Override value \n") or False
        if proceed in {"N","n"}:
            abort()

    except TimeoutOccurred:
        Override = False

    # Skip on holidays and weekends
    if date.today().weekday() >= 5 or CheckForDateHoliday(date.today()):
        dayType = "weekend" if date.today().weekday() >= 5 else f"holiday ({date.today()})"
        print(f"[MONTHLY FNO] {dayType} — skipping order placement")
        exit(0)

    print('Waiting to hit the entry time')
    while one_shot_flag == True:
        PrevWkDy = datetime.datetime.now().weekday() - 1
        CurrWkDy = datetime.datetime.now().weekday()

        now = datetime.datetime.now()

        BankNiftyStraddle_Fri_945Am_Monthly =  str(now.strftime("%H:%M:%S")) == '09:45:00' and ((CurrWkDy == FRIDAY)   or (PrevWkDy == FRIDAY and CheckForDateHoliday(PREVIOUSDATE)))
        BankNiftyStraddle_Fri_945Am_Monthly_Exit =  str(now.strftime("%H:%M:%S")) == '15:15:00' and ((CurrWkDy == THURSDAY)   or (PrevWkDy == THURSDAY and CheckForDateHoliday(PREVIOUSDATE)))
        MidCPNiftyStraddle_Wed_13Pm_Monthly =   str(now.strftime("%H:%M:%S")) == '13:00:00' and ((CurrWkDy == WEDNESDAY)or (PrevWkDy == WEDNESDAY and CheckForDateHoliday(PREVIOUSDATE)))
        MidCPNiftyStraddle_Wed_13Pm_Monthly_Exit =   str(now.strftime("%H:%M:%S")) == '15:15:00' and ((CurrWkDy == MONDAY)or (PrevWkDy == MONDAY and CheckForDateHoliday(PREVIOUSDATE)))
        BankNiftyMACD_Thu_15Pm_Monthly_Exit =    str(now.strftime("%H:%M:%S")) == '15:15:00' and ((CurrWkDy == WEDNESDAY)or (PrevWkDy == WEDNESDAY and CheckForDateHoliday(PREVIOUSDATE)))
        MidCPNiftyMACD_Mon_15Pm_Monthly_Exit =  str(now.strftime("%H:%M:%S")) == '15:20:00' and ((CurrWkDy == WEDNESDAY)or (PrevWkDy == WEDNESDAY and CheckForDateHoliday(PREVIOUSDATE)))


        # For testing the override scenario:
        if  BankNiftyStraddle_Fri_945Am_Monthly or Override == '95':
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'BANKNIFTY', 'Quantity': '30', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0,
                     'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ZERODHA_OPTION','Netposition':'','OptionExpiryDay':'1','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'',
                     'StopLossOrderPlacePercent':'','CallStrikeRequired':'True','PutStrikeRequired':'True','Hedge':'False','OptionType':'MonthlyOption','Exit':'False',"OrderTag":"21BN-STR-TH"}}
            one_shot_flag = False
            Override = False
            break

            # Set SL based on week logic
        if  BankNiftyStraddle_Fri_945Am_Monthly_Exit or Override == '96':
            OrderDetails = {'Straddle':{'Tradetype': 'BUY', 'Exchange': 'NFO', 'Tradingsymbol': 'BANKNIFTY', 'Quantity': '30', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0,
                'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ZERODHA_OPTION','Netposition':'','OptionExpiryDay':'1','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'',
                'StopLossOrderPlacePercent':'','CallStrikeRequired':'False','PutStrikeRequired':'False','Hedge':'False','OptionType':'MonthlyOption','Exit':'True',"OrderTag":"21BN-STR-TH"}}

            one_shot_flag = False
            Override = False
            break

        if  MidCPNiftyStraddle_Wed_13Pm_Monthly or Override == '97':
            OrderDetails = {'Straddle':{'Tradetype': 'SELL', 'Exchange': 'NFO', 'Tradingsymbol': 'MIDCPNIFTY', 'Quantity': '120', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0,
                'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ZERODHA_OPTION','Netposition':'','OptionExpiryDay':'1','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'',
                'StopLossOrderPlacePercent':'','CallStrikeRequired':'True','PutStrikeRequired':'True','Hedge':'False','OptionType':'MonthlyOption','Exit':'False',"OrderTag":"22MN-STR-TH"}}

            one_shot_flag = False
            Override = False
            break

        if  MidCPNiftyStraddle_Wed_13Pm_Monthly_Exit or Override == '98':
            OrderDetails = {'Straddle':{'Tradetype': 'BUY', 'Exchange': 'NFO', 'Tradingsymbol': 'MIDCPNIFTY', 'Quantity': '120', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0,
                'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ZERODHA_OPTION','Netposition':'','OptionExpiryDay':'1','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'',
                'StopLossOrderPlacePercent':'','CallStrikeRequired':'False','PutStrikeRequired':'False','Hedge':'False','OptionType':'MonthlyOption','Exit':'True',"OrderTag":"22MN-STR-TH"}}

            one_shot_flag = False
            Override = False
            break
        
        if  BankNiftyMACD_Thu_15Pm_Monthly_Exit or Override == '99':
            OrderDetails = {'Straddle':{'Tradetype': 'BUY', 'Exchange': 'NFO', 'Tradingsymbol': 'BANKNIFTY', 'Quantity': '30', 'Variety': 'REGULAR', 'Ordertype': 'MARKET', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 0,
                'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ZERODHA_OPTION','Netposition':'','OptionExpiryDay':'1','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'',
                'StopLossOrderPlacePercent':'','CallStrikeRequired':'False','PutStrikeRequired':'False','Hedge':'False','OptionType':'MonthlyOption','Exit':'True',"OrderTag":"25BN-SP-MD-TH"}}

            one_shot_flag = False
            Override = False
            break

        if  MidCPNiftyMACD_Mon_15Pm_Monthly_Exit or Override == '100':
            OrderDetails = {'Straddle':{'Tradetype': 'BUY', 'Exchange': 'NFO', 'Tradingsymbol': 'MIDCPNIFTY', 'Quantity': '120', 'Variety': 'AMO', 'Ordertype': 'LIMIT', 'Product': 'NRML', 'Validity': 'DAY', 'Price': 350,
                'Symboltoken':'', 'Squareoff':'', 'Stoploss':'','Broker':'ZERODHA_OPTION','Netposition':'','OptionExpiryDay':'1','OptionContractStrikeFromATMPercent':'0','Trigger':'1','StopLossTriggerPercent':'',
                'StopLossOrderPlacePercent':'','CallStrikeRequired':'False','PutStrikeRequired':'False','Hedge':'False','OptionType':'MonthlyOption','Exit':'True',"OrderTag":""}}

            one_shot_flag = False
            Override = False
            break
    #Add condition here for monthly option check and exit
    
        
    for orderinfo in OrderDetails:
        ExitFlag = OrderDetails[orderinfo]['Exit']

    if ExitFlag == 'False':
        set_week_based_sl(OrderDetails)

    if ExitFlag == 'True':
        # Process exit orders logic is now in a separate function
        process_exit_orders(OrderDetails, kite, WriteOptionDetailsFile)
    else:
        #exit(0)
        LoopHashOrderRequest(OrderDetails)
