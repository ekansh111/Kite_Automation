"""
This script demonstrates how to place orders with Zerodha (Kite API) using Python.
It includes functionality for:
- Reading an instrument details file (CSV) and filtering based on certain criteria (like expiry date).
- Establishing a connection/session to the Kite Connect using user credentials.
- Validating and preparing order details (e.g., setting limit price to LTP if Ordertype != MARKET).
- Placing limit or market orders, with a potential fallback to convert unfilled limit orders to market orders.
- Handling contract rollover logic for futures based on a specified RolloverDate.
"""

# package import statement
import logging
import time
from datetime import date, datetime, timedelta
import pandas as pd
import pytz
from Directories import *
from Fetch_Positions_Data import get_order_status
from Server_Order_Place import *
# For Kite Connect
from kiteconnect import KiteConnect

_KiteHandlerLogger = logging.getLogger(__name__)


def _ComputeTradingDaysRolloverDate(Today, N, Exchange):
    """Return the calendar date that is N trading days after Today.

    Uses the exchange-aware holiday calendar so weekends and MCX/NSE
    holidays do not shorten the window.  Matches the trading-day counter
    that rollover_monitor uses when deciding whether to fire a rollover,
    so the new-order picker and the rollover engine agree on "3 days
    before expiry".
    """
    # Deferred import keeps import graph light and avoids a hard dependency
    # at module load time (some test harnesses stub Holidays only after
    # importing this module).
    from Holidays import CheckForDateHoliday

    try:
        N = int(N)
    except (TypeError, ValueError):
        N = 0
    if isinstance(Today, datetime):
        Current = Today
    else:
        Current = datetime.combine(Today, datetime.min.time())
    Remaining = N
    while Remaining > 0:
        Current = Current + timedelta(days=1)
        if Current.weekday() >= 5:
            continue
        if CheckForDateHoliday(Current.date(), exchange=Exchange):
            continue
        Remaining -= 1
    return Current


def _FindPinnedRolloverContractKite(OrderDetails, ZerodhaInstrumentDetails, Today):
    """If a rollover has already completed for this instrument, return the CSV
    row for the new_contract (as a single-row DataFrame).  Returns an empty
    DataFrame if no pin applies.

    The rollover_log stores the NEW contract symbol after a successful
    two-leg rollover.  Without this pin, the legacy filter
    (expiry > RolloverDate) could silently re-select the just-rolled-out
    front-month whenever the calendar-day RolloverDate falls before the
    actual expiry (e.g. across weekends).
    """
    try:
        import forecast_db as _db  # deferred to keep import graph light
        Rows = _db.GetRecentCompletedRollovers(limit=30, Broker='ZERODHA')
    except Exception as Exc:
        _KiteHandlerLogger.warning("Rollover DB lookup failed: %s", Exc)
        return pd.DataFrame()

    if not Rows:
        return pd.DataFrame()

    TargetName = OrderDetails.get('Tradingsymbol')
    TargetExchange = OrderDetails.get('Exchange')
    TargetInstType = OrderDetails.get('InstrumentType')

    for Row in Rows:
        NewContract = Row.get('new_contract')
        if not NewContract:
            continue
        Match = ZerodhaInstrumentDetails[
            (ZerodhaInstrumentDetails['symbol'] == NewContract) &
            (ZerodhaInstrumentDetails['name'] == TargetName) &
            (ZerodhaInstrumentDetails['exch_seg'] == TargetExchange) &
            (ZerodhaInstrumentDetails['instrumenttype'] == TargetInstType) &
            (ZerodhaInstrumentDetails['expiry'] > Today)
        ]
        if not Match.empty:
            _KiteHandlerLogger.info(
                "Pinning order for %s to rolled-over contract %s (DB row id=%s)",
                TargetName, NewContract, Row.get('id')
            )
            return Match.sort_values(by='expiry', ascending=True).head(1)
    return pd.DataFrame()

# Types of Orders
LimitOrder = 'LIMIT'
MarketOrder = 'MARKET'


def ConfigureNetDirectionOfTrade(OrderDetails):
    """
    Sets the 'NetDirection' key in OrderDetails based on the 'Tradetype'.
    If Tradetype is BUY (case-insensitive), NetDirection is set to 1.
    If Tradetype is SELL (case-insensitive), NetDirection is set to -1.
    """
    if OrderDetails['Tradetype'].strip().upper() == 'BUY':
        OrderDetails['NetDirection'] = 1
    elif OrderDetails['Tradetype'].strip().upper() == 'SELL':
        OrderDetails['NetDirection'] = -1
    return OrderDetails


def PrepareInstrumentContractNameKite(kite,OrderDetails):
    """
    Calls the function that filters instrument contracts based on certain criteria
    (like expiry date, symbol, etc.), then updates OrderDetails with the selected contract info.
    """

    ZerodhaInstrument_filtered = PrepareKiteInstrumentContractName(kite,OrderDetails)
    #print('Zerodha instrument filtered')
    #print(ZerodhaInstrument_filtered)
    UpdateRequestContractDetailsKite(OrderDetails, ZerodhaInstrument_filtered)

    return OrderDetails


def PrepareKiteInstrumentContractName(kite,OrderDetails):
    """
    Reads the instrument details from a CSV file (ZerodhaInstrumentDirectory),
    applies filtering logic based on OrderDetails, and returns the filtered DataFrame.
    """

    # Read the CSV file into a DataFrame
    ZerodhaInstrumentDetails = pd.read_csv(ZerodhaInstrumentDirectory, delimiter=',')
    # Rename the unnamed column to 'serialnumber' if it exists
    ZerodhaInstrumentDetails.rename(columns={'Unnamed: 0': 'serialnumber'}, inplace=True)

    # Current datetime for reference
    today = datetime.now()

    # Compute the rollover date by adding N *trading* days (not calendar days) so
    # that weekends and exchange holidays don't shrink the window below what the
    # rollover_monitor used when deciding to fire.  This prevents the front-month
    # contract from being re-selected after it has already been rolled out.
    RolloverDate = _ComputeTradingDaysRolloverDate(
        today,
        OrderDetails['DaysPostWhichSelectNextContract'],
        OrderDetails.get('Exchange'),
    )

    # Convert the 'expiry' column to a datetime. Example format: '2025-02-28' => datetime object
    ZerodhaInstrumentDetails['expiry'] = pd.to_datetime(
        ZerodhaInstrumentDetails['expiry'].str.title(),
        format='%Y-%m-%d', # The date format might differ; adjust as needed
        errors='coerce'
    )
    #print(ZerodhaInstrumentDetails)

    # If rollover_monitor has already completed a rollover for this instrument,
    # pin this order to the new_contract so we never accidentally place on the
    # just-closed front-month.  The DB is authoritative once the rollover row
    # is COMPLETE.
    PinnedMatch = _FindPinnedRolloverContractKite(
        OrderDetails, ZerodhaInstrumentDetails, today
    )
    if not PinnedMatch.empty:
        return PinnedMatch

    ZerodhaInstrumentDetails_filtered = pd.DataFrame()

    # If the net position does not match the quantity or if we're re-entering the order loop,
    # we check for existing old contracts to square off.
    if ((int(OrderDetails['Netposition']) != int(OrderDetails['Quantity'])) or (OrderDetails.get('ReEnterOrderLoop') == 'True')):
        
        # If there is no net position, check if there's an old contract to square off.
        if int(OrderDetails['Netposition']) == 0:
            ZerodhaInstrumentDetails_filtered = CheckIfExistingOldContractSqOffReq(
                kite,ZerodhaInstrumentDetails,OrderDetails,today,RolloverDate
            )

        else:
            # If ReEnterOrderLoop is True, update the quantity info accordingly.
            if OrderDetails.get('ReEnterOrderLoop') == 'True':
                OrderDetails['Quantity'] = OrderDetails['QuantityToBePlacedInNextRound']
                OrderDetails['ReEnterOrderLoop'] == 'False'
                OrderDetails['Tradingsymbol'] = OrderDetails['InitialTradingsymbol']

            else:
                OrderDetails['InitialTradingsymbol'] = OrderDetails['Tradingsymbol']

                ZerodhaInstrumentDetails_filtered = CheckIfExistingOldContractSqOffReq(
                    kite,ZerodhaInstrumentDetails,OrderDetails,today,RolloverDate
                )
                #print(ZerodhaInstrumentDetails_filtered)
                if not ZerodhaInstrumentDetails_filtered.empty:
                    OrderDetails['ReEnterOrderLoop'] = 'True'

                    # Calculate how many contracts are in the old month vs new month
                    NoOfContractsInOldMonthFormat = int(ZerodhaInstrumentDetails_filtered['quantity'].iloc[0])
                    NoOfContractsInNewMonthFormatToPlaceOrders = int(OrderDetails['Quantity']) 

                    # If new month quantity > old month quantity, figure out how many are needed in each step
                    if NoOfContractsInNewMonthFormatToPlaceOrders > NoOfContractsInOldMonthFormat:
                        InitialOrderQuantity = NoOfContractsInOldMonthFormat
                        NetQuantityOrdersToBePlaced = NoOfContractsInNewMonthFormatToPlaceOrders - abs(NoOfContractsInOldMonthFormat)

                    else:
                        InitialOrderQuantity = NoOfContractsInNewMonthFormatToPlaceOrders
                        NetQuantityOrdersToBePlaced = NoOfContractsInOldMonthFormat - abs(NoOfContractsInNewMonthFormatToPlaceOrders)

                    # Make sure initial order quantity is non-negative
                    if InitialOrderQuantity < 0:
                        InitialOrderQuantity = -InitialOrderQuantity

                    OrderDetails['Quantity'] = InitialOrderQuantity
                    OrderDetails['QuantityToBePlacedInNextRound'] = NetQuantityOrdersToBePlaced  

    # If no old contract was found or the filtered DataFrame is empty,
    # pick the new contract with expiry > RolloverDate.
    if ZerodhaInstrumentDetails_filtered.empty:
        ZerodhaInstrumentDetails_filtered = ZerodhaInstrumentDetails[
            (ZerodhaInstrumentDetails['name'] == OrderDetails['Tradingsymbol']) &
            (ZerodhaInstrumentDetails['exch_seg'] == OrderDetails['Exchange']) &
            (ZerodhaInstrumentDetails['instrumenttype'] == OrderDetails['InstrumentType']) &
            (ZerodhaInstrumentDetails['expiry'] > RolloverDate)
        ].sort_values(by='expiry', ascending=True).head(1)

    return ZerodhaInstrumentDetails_filtered


def CheckIfExistingOldContractSqOffReq(kite, ZerodhaInstrumentDetails, OrderDetails, today, RolloverDate):
    """
    Checks if an existing old contract needs to be squared off before the rollover date.
    Filters available contracts based on the symbol, exchange, instrument type, and expiry range.
    Then checks the user's Kite positions to see if there's a matching contract that requires closure.
    """
    # Step 1: Filter the contracts based on the given criteria
    ZerodhaInstrumentDetails_filtered = ZerodhaInstrumentDetails[
        (ZerodhaInstrumentDetails['name'] == OrderDetails['Tradingsymbol']) &
        (ZerodhaInstrumentDetails['exch_seg'] == OrderDetails['Exchange']) &
        (ZerodhaInstrumentDetails['instrumenttype'] == OrderDetails['InstrumentType']) &
        (ZerodhaInstrumentDetails['expiry'] >= today) &
        (ZerodhaInstrumentDetails['expiry'] <= RolloverDate)
    ].sort_values(by='expiry', ascending=True).head(1)

    # Step 2: Check if any matching contract exists
    if not ZerodhaInstrumentDetails_filtered.empty:
        # Fetch existing positions from Kite for the given order details
        KitePositions = FetchExistingNetKitePositions(kite, OrderDetails)
        #print('kite positions')
        #print(KitePositions)

        # Determine the comparison condition based on Tradetype
        if str(OrderDetails['Tradetype']).upper() == 'BUY':
            comparison_condition = (KitePositions['quantity'] < OrderDetails['NetDirection'])
        else:
            comparison_condition = (KitePositions['quantity'] > OrderDetails['NetDirection'])

        # Further filter the Kite positions to match the selected contract's symbol and token
        KitePositionsFiltered = KitePositions[
            (KitePositions['tradingsymbol'] == ZerodhaInstrumentDetails_filtered['symbol'].iloc[0]) &
            (KitePositions['instrument_token'] == ZerodhaInstrumentDetails_filtered['token'].iloc[0]) &
            (KitePositions['quantity'] != 0) &
            comparison_condition
        ].copy()

        # Rename columns in the copied DataFrame
        KitePositionsFiltered.rename(columns={'tradingsymbol': 'symbol', 'instrument_token': 'token'}, inplace=True)

        # Step 3: If there are matching positions in the OLD contract, return
        # those.  Previously this checked KitePositions.empty (the full positions
        # DataFrame), which was True whenever the user held any position on Kite
        # — including the already-rolled-over new month — causing this function
        # to return an empty DF that fell through to the buggy calendar-day
        # filter in the caller.
        if not KitePositionsFiltered.empty:
            return KitePositionsFiltered
        # If the user has positions but none match the old contract criteria, return empty
        return pd.DataFrame()
    else:
        # No matching old contract found
        return pd.DataFrame()


def FetchExistingNetKitePositions(kite,OrderDetails):
    """
    Fetches the net positions from Kite and returns them as a DataFrame.
    """
    # Positions is a dict with keys: 'net' and 'day'
    positions = kite.positions()
    
    # Extract net positions list
    net_positions = positions['net']
    ZerodhaInstrument_positions = pd.DataFrame(net_positions)

    return ZerodhaInstrument_positions


def UpdateRequestContractDetailsKite(OrderDetails, ZerodhaInstrument_filtered):
    """
    Updates OrderDetails with the symbol and token from the filtered DataFrame.
    """

    OrderDetails['Tradingsymbol'] = ZerodhaInstrument_filtered['symbol'].iloc[0]
    OrderDetails['Symboltoken']   = ZerodhaInstrument_filtered['token'].iloc[0]

    return OrderDetails


def EstablishConnectionKiteAPI(OrderDetails):
    """
    Reads credentials from a file (e.g., line 1: api_key, line 2: request_token, line 3: api_secret),
    then sets up the KiteConnect object with the access_token.
    """
    if str(OrderDetails.get('User')) == 'IK6635':
        APIKeyDirectory = KiteEkanshLogin
        AccessTokenDirectory = KiteEkanshLoginAccessToken

    elif str(OrderDetails.get('User')) == 'YD6016':  
        APIKeyDirectory = KiteRashmiLogin
        AccessTokenDirectory = KiteRashmiLoginAccessToken

    with open(APIKeyDirectory,'r') as InputsFile:
        content = InputsFile.readlines()
        api_key   = content[2].strip('\n')
        InputsFile.close()

    kite = KiteConnect(api_key=api_key)

    with open(AccessTokenDirectory,'r') as f:
        access_tok = f.read()
        f.close()

    kite.set_access_token(access_tok)
    return kite


def Validate_Quantity(OrderDetails):
    """
    If the quantity is specified in the format "2*50", parse the multiplier
    and adjust both Quantity and Netposition accordingly.
    """
    Quantitysplit = str(OrderDetails['Quantity']).split('*')

    if len(Quantitysplit) > 1:
        UpdatedQuantity = int(Quantitysplit[0]) * int(Quantitysplit[1])
        UpdatedNetQuantity = int(OrderDetails['Netposition']) * int(Quantitysplit[1])

        OrderDetails['Quantity'] = UpdatedQuantity 
        OrderDetails['Netposition'] = UpdatedNetQuantity

    return OrderDetails


def PlaceOrderKiteAPI(kite, OrderDetails):
    """
    Places the order using the Kite Connect API. Modify parameters as needed.
    """
    #print('Order details in PlaceOrderKiteAPI:')
    #print(OrderDetails)
    
    order_id = order(OrderDetails)

    return order_id


def ConvertToMarketOrder(kite, OrderDetails):
    """
    Converts an existing order's details to a MARKET order
    and places the order again if the limit order didn't fill.
    """
    OrderDetails['Price']     = 0.0
    OrderDetails['Ordertype'] = MarketOrder
    return PlaceOrderKiteAPI(kite, OrderDetails)


def SleepForRequiredTime(SleepTime):
    """
    Pauses execution for the specified number of seconds.
    """
    time.sleep(SleepTime)
    return True


def PrepareOrderKite(kite, OrderDetails):
    """
    Fetches LTP data and sets the limit price if the order type is not MARKET.
    """
    exchange_symbol = f"{OrderDetails['Exchange']}:{OrderDetails['Tradingsymbol']}"
    try:
        ltp_data = kite.ltp([exchange_symbol])
        instrument_ltp = ltp_data[exchange_symbol]['last_price']
        print("LTP Info:", instrument_ltp)

        if OrderDetails['Ordertype'] != 'MARKET':
            OrderDetails['Price'] = instrument_ltp
    except Exception as e:
        print("Error fetching LTP data:", e)

    return OrderDetails


def ControlOrderFlowKite(OrderDetails):
    """
    Orchestrates the entire order flow for Kite, from contract selection
    to final order placement. It handles limit orders, optionally converts
    unfilled limit orders to market, and checks for re-entry logic.
    """

    # 1. Create a Kite Connect session
    kite = EstablishConnectionKiteAPI(OrderDetails)

    # 2. Configure net trade direction (for partial contract logic)
    ConfigureNetDirectionOfTrade(OrderDetails)
    
    # 3. Validate and fix quantity if needed
    Validate_Quantity(OrderDetails)
    
    # 4. If the contract name is not directly provided, figure it out.
    if OrderDetails['ContractNameProvided'] == 'False':
        PrepareInstrumentContractNameKite(kite,OrderDetails)


    # 5. Optionally fetch LTP and set the limit price if not a market order
    OrderDetails = PrepareOrderKite(kite, OrderDetails)

    # 6. Place the (possibly) limit order
    order_id = PlaceOrderKiteAPI(kite, OrderDetails)

    # 7. If it’s a MARKET order, we may be done or handle re-entry logic
    if OrderDetails['Ordertype'].upper() == 'MARKET':
        if OrderDetails.get('ReEnterOrderLoop') == 'True':

            if OrderDetails['ContractNameProvided'] == 'False':
                PrepareInstrumentContractNameKite(kite,OrderDetails)
            
            # Fetch LTP if not market
            OrderDetails = PrepareOrderKite(kite, OrderDetails)

            # Place the new order
            order_id = PlaceOrderKiteAPI(kite, OrderDetails)
            
            return order_id
        return order_id
    else:
        # 8. If this is a LIMIT order and ConvertToMarketOrder is True,
        #    we wait some time and then possibly convert to market.
        order_list = []
        order_list.append(order_id)
        #print(order_list)

        if OrderDetails['ConvertToMarketOrder'] == 'True':
            if int(OrderDetails['Netposition']) != 0:
                print(f'Waiting for {OrderDetails["EntrySleepDuration"]} seconds')
                SleepForRequiredTime(int(OrderDetails['EntrySleepDuration']))
            else:
                print(f'Waiting for {OrderDetails["ExitSleepDuration"]} seconds')
                SleepForRequiredTime(int(OrderDetails['ExitSleepDuration']))
            
            OrderType = 'MARKET'
            ReorderFlag = 1
            get_order_status(kite, order_list, OrderType, ReorderFlag)

            if OrderDetails.get('ReEnterOrderLoop') == 'True':
                OrderDetails['Ordertype'] = 'LIMIT'
                if OrderDetails['ContractNameProvided'] == 'False':
                    PrepareInstrumentContractNameKite(kite,OrderDetails)
                
                # Possibly fetch LTP again
                OrderDetails = PrepareOrderKite(kite, OrderDetails)

                # Place the new limit order
                order_id = PlaceOrderKiteAPI(kite, OrderDetails)
                order_list = []
                order_list.append(order_id)

                print(f'Waiting for {OrderDetails["EntrySleepDuration"]} seconds')
                SleepForRequiredTime(int(OrderDetails['EntrySleepDuration']))
                get_order_status(kite, order_list, OrderType, ReorderFlag)
                return order_id

        return order_id
