#from calendar import Wednesday
import logging
from os import abort
import string
from kiteconnect import KiteConnect
from datetime import datetime,timedelta
from datetime import date
from ContractDetails import ContractStrikeValue
from Set_Gtt_Exit import Set_Gtt
from inputimeout import inputimeout,TimeoutOccurred
from dateutil.relativedelta import TH,MO,TU,WE,FR, relativedelta
import time
from Holidays import CheckForDateHoliday
from Directories import *
import calendar
from datetime import date
from dateutil.relativedelta import relativedelta

#Function to fetch the option name of the script
def FetchOptionName(OrderDetails):

    #print(OrderDetails)
    #Fetch the required varables from the hash
    IndexName                    = OrderDetails['Tradingsymbol']
    ExpiryDayInt                 = int(OrderDetails['OptionExpiryDay'])
    ContractStrikeFromATMPercent = int(OrderDetails['OptionContractStrikeFromATMPercent'])
    Hedge                        = OrderDetails['Hedge']
    CE_Return                    = OrderDetails['CallStrikeRequired']
    PE_Return                    = OrderDetails['PutStrikeRequired']
    Broker                       = OrderDetails.get("Broker")
    OptionType                   = OrderDetails.get("OptionType")
    #order_details_fetch.get("Broker") == 'ANGEL':

    #Code block to check if the expiry day is a holiday or not###########################################################################################################################################################################################
    ExpiryDate = date.today() 
    while ExpiryDate.weekday() != ExpiryDayInt:
        ExpiryDate += timedelta(1)

    IdealExpiryDateForContract = ExpiryDate
    #print(IdealExpiryDateForContract)
    CheckIfExpiryDateIsHoliday = CheckForDateHoliday(IdealExpiryDateForContract)
    #print(CheckIfExpiryDateIsHoliday)
    if str(CheckIfExpiryDateIsHoliday) == 'True':
        #If the Expiry date is on Monday then the contract will expire on friday
        if ExpiryDayInt == 0:
            ExpiryDayInt = 4
            if Broker == 'ANGEL':
                GTTOrderTimePeriodExpiryDay = int(OrderDetails.get("TimePeriod"))
                GTTOrderTimePeriodExpiryDay = GTTOrderTimePeriodExpiryDay - 3
                OrderDetails['TimePeriod']  = GTTOrderTimePeriodExpiryDay   
        else:
            #If the expiry day is a holiday then fetch the prior date
            ExpiryDayInt = ExpiryDayInt - 1
            
            if Broker == 'ANGEL':
                GTTOrderTimePeriodExpiryDay = int(OrderDetails.get("TimePeriod"))
                GTTOrderTimePeriodExpiryDay = GTTOrderTimePeriodExpiryDay - 1
                OrderDetails['TimePeriod']  = GTTOrderTimePeriodExpiryDay 
    ######################################################################################################################################################################################################################################################

    #Contract Strike should not be overriden by default, Unless specified in the request
    ContractStrikeOverride = 'False'

    ContractStrikeOverridePrice  = False or OrderDetails.get("ContractStrikeOverridePrice")
    
    #If the ContractStrikeOverridePrice is not entry and is not null, then in that case it must be having the ltp ovveride value for 
    #the option entry contract
    if str(OrderDetails.get("ContractStrikeOverridePrice")) != 'Entry' and str(OrderDetails.get("ContractStrikeOverridePrice")) != str(None):
        ContractStrikeOverride       = 'True'
        #ltp = ContractStrikeOverridePrice


    #print(IndexName,ExpiryDayInt,ContractStrikeFromATMPercent,Hedge,CE_Return,PE_Return)
    #Configure the API for fetching the ltp value
    with open(KiteEshitaLogin,'r') as a:
        content = a.readlines()
        a.close()
    api_key = content[2].strip('\n')
    kite = KiteConnect(api_key=api_key)



    with open(KiteEshitaLoginAccessToken,'r') as f:
        access_tok = f.read()
        f.close()
        #print(access_tok)
    kite.set_access_token(access_tok)

    #Get the date of this year ,month and day
    ExpiryDate = date.today()
    
    #If the contract expiry weekday is the same as the weekday on which the trade is placed, then fetch the next weekend date,
    #to place order in the corresponding next weekday contract.
    if  ExpiryDate.weekday() == ExpiryDayInt:
        ExpiryDate += timedelta(7)

    # weekday() can be used to retrieve the day of the week. The datetime.today() method returns the current date, and the weekday() method returns the day of the week as an integer where Monday is indexed as 0 and Sunday is 6.
    #Fetch the expiry date for the contract
    while ExpiryDate.weekday() != ExpiryDayInt:
        #Since the options expire on Expiry their namefield will have the corresponding date field of that day
        ExpiryDate += timedelta(1)  

    #For the provided expiry day(in int) fetch the day of the week in char format and then fetch the last date for the final instance of the particular day in the month
    HashOfDays = {0:MO,1:TU,2:WE,3:TH,4:FR}
    ExpiryDay = HashOfDays[ExpiryDayInt]

    #Fetch the last Expiry date of the month,Needs correction for various different contracts
    last_day = (date.today()+relativedelta(day=31, weekday=ExpiryDay(-1)))
    
    #if the last Expiry day comes in next year then the year will be rolled over
    y0= ExpiryDate.strftime("%y")
    
    #No longeer in use as the format has been changed #find out using the days left to next month, if the next month value needs to be added. This can happen during the last week of the month when the next month's weekly contract needs to selected.
    m0 = ExpiryDate.strftime("%m")
    
    #month has to be converted into an integer because it cannot have 0 suffixing it if it is a single digit month eg in the contract    
    month = int(m0)#[0]#Get the first letter of the month for new format

    if month > 9:
        m0 = (ExpiryDate.strftime("%b")).upper()
        print('First day of the month' + str(m0))
        month = m0[0]

    #m0 = (ExpiryDate.strftime("%b")).upper()#Fetch the name of the month of the option series to be executed
    d0= ExpiryDate.strftime("%d")
    year = int(y0)
    day=d0

    #Check if the day is last Expiry day of the month (Since Expiry day is the expiry day for options)
    #Even if the last day turns out to be an holiday, no issues since the prior day will also be last instance for that month and Monthly contract name willl be used
    #Post dec 2024 , even Angel will need same changes
    if (ExpiryDate == last_day): #and (Broker != 'ANGEL'):
    #If true then need to change the value for month and day as the name format of the option contract(Month expiry Option ) changes
        month=(date.today().strftime("%b")).upper()
        day=''
    ########################################################################################################################################################################################################################

    #If the entry needs to be taken in the next month Call option, Then fetch the next month name for contract 
    if(Hedge == 'MonthlyCall') and (Broker != 'ANGEL'):
        #Fetch the next month name
        month=((date.today() + relativedelta(months=1)).strftime("%b") ).upper()
        day=''

    if OptionType == 'MonthlyOption':
        today = date.today()
        y = today.year
        m = today.month
        
        # Determine the last Thursday of the current month
        last_day = calendar.monthrange(y, m)[1]  # Last day of the current month
        last_thursday = None
        for d in range(last_day, 0, -1):
            dt = date(y, m, d)
            # weekday(): Monday=0, Tuesday=1, ..., Thursday=3
            if dt.weekday() == 3:
                last_thursday = dt
                break

        # If today is before the last Thursday, choose the current month.
        # If today is on or after the last Thursday, choose the next month.
        if today < last_thursday:
            # Current month name in uppercase short form
            month = today.strftime("%b").upper()
        else:
            # Next month's name
            month = ((today + relativedelta(months=1)).strftime("%b")).upper()

        day = ''


    #print(kite.instruments(exchange='NSE'))
    #Fetch the LTP based on the Indexes for preparing the contracts
    if IndexName == 'BANKNIFTY' and (ContractStrikeOverride != 'True'):
        ##################################################################################################################################
        #Find the current value of the bank nifty index
        Banknifty_index = {260105:'NIFTY BANK'}
        
        for val in Banknifty_index:
            time.sleep(0.1)
            #this will send ohlc price in dictionary format
            price = kite.ltp('NSE:' + Banknifty_index[val])
            #print(price)
            #to get ltp of whichever stick is declared in token
            ltp = price['NSE:'+Banknifty_index[val]]['last_price']
            print("BankNifty LTP:"+str(ltp))            

        ##################################################################################################################################

    elif IndexName == 'NIFTY' and (ContractStrikeOverride != 'True'):
        ##################################################################################################################################
        #Find the current value of the bank nifty index
        NIFTY_index = {256265:'NIFTY 50'}
        
        for val in NIFTY_index:
            time.sleep(0.1)
            price = kite.ltp('NSE:' + NIFTY_index[val])#this will send ohlc price in dictionary format
            #print(price)
            ltp = price['NSE:'+NIFTY_index[val]]['last_price']#to get ltp of whichever stick is declared in token
            print("NIFTY LTP:"+str(ltp))
                

        ##################################################################################################################################

    elif IndexName == 'FINNIFTY' and (ContractStrikeOverride != 'True'):
        ##################################################################################################################################
        #Find the current value of the bank nifty index
        NIFTY_index = {257801:'NIFTY FIN SERVICE'}
        
        for val in NIFTY_index:
            time.sleep(0.1)
            price = kite.ltp('NSE:' + NIFTY_index[val])#this will send ohlc price in dictionary format
            #print(price)
            ltp = price['NSE:'+NIFTY_index[val]]['last_price']#to get ltp of whichever stick is declared in token
            print("FINNIFTY LTP:"+str(ltp))
                

        ##################################################################################################################################

    elif IndexName == 'MIDCPNIFTY' and (ContractStrikeOverride != 'True'):
        ##################################################################################################################################
        #Find the current value of the bank nifty index
        NIFTY_index = {288009:'NIFTY MID SELECT'}
        
        for val in NIFTY_index:
            time.sleep(0.1)
            price = kite.ltp('NSE:' + NIFTY_index[val])#this will send ohlc price in dictionary format
            #print(price)
            ltp = price['NSE:'+NIFTY_index[val]]['last_price']#to get ltp of whichever stick is declared in token
            print("MIDCPNIFTY LTP:"+str(ltp))
                

        ##################################################################################################################################     

    elif IndexName == 'SENSEX' and (ContractStrikeOverride != 'True'):
        ##################################################################################################################################
        #Find the current value of the bank nifty index
        NIFTY_index = {265:'SENSEX'}
        
        for val in NIFTY_index:
            time.sleep(0.1)
            price = kite.ltp('BSE:' + NIFTY_index[val])#this will send ohlc price in dictionary format
            #print(price)
            ltp = price['BSE:'+NIFTY_index[val]]['last_price']
            print("SENSEX LTP:"+str(ltp))
        ##################################################################################################################################      

    #Override the ltp value If the contract name needs to be Named according to ltp(when the contract position was entered) provided in the request
    if ContractStrikeOverride == 'True':
        ltp = float(ContractStrikeOverridePrice)

    ATM_ltp = int(round(ltp,-2))
    ATM_CE_Strike,ATM_PE_Strike = ContractStrikeValue(ContractStrikeFromATMPercent,ATM_ltp,IndexName)

    DateFormat = str(year)+ str(month) +str(day)

    if Broker == 'ANGEL':
        if IndexName != 'SENSEX':
            month = (ExpiryDate.strftime("%b")).upper()
            DateFormat = str(day)+str(month)+str(year)
        else:
            DateFormat = str(year) +str(month) + str(day)

    if IndexName == 'BANKNIFTY':
        ATM_CALL = 'BANKNIFTY'+ DateFormat +str(ATM_CE_Strike)+'CE'
        ATM_PUT = 'BANKNIFTY'+ DateFormat +str(ATM_PE_Strike)+'PE'

    elif IndexName == 'NIFTY':
        ATM_CALL = 'NIFTY'+ DateFormat +str(ATM_CE_Strike)+'CE'
        ATM_PUT = 'NIFTY'+ DateFormat +str(ATM_PE_Strike)+'PE'
    
    elif IndexName == 'FINNIFTY':
        ATM_CALL = 'FINNIFTY'+ DateFormat +str(ATM_CE_Strike)+'CE'
        ATM_PUT = 'FINNIFTY'+ DateFormat +str(ATM_PE_Strike)+'PE' 

    elif IndexName == 'MIDCPNIFTY':
        ATM_CALL = 'MIDCPNIFTY'+ DateFormat +str(ATM_CE_Strike)+'CE'
        ATM_PUT = 'MIDCPNIFTY'+ DateFormat +str(ATM_PE_Strike)+'PE'

    elif IndexName == 'SENSEX':
        ATM_CALL = 'SENSEX'+ DateFormat +str(ATM_CE_Strike)+'CE'
        ATM_PUT = 'SENSEX'+ DateFormat +str(ATM_PE_Strike)+'PE'

    print(year,month,day,ATM_ltp,ATM_CALL,ATM_PUT,CE_Return,PE_Return)

    if CE_Return == 'True' and PE_Return=='False':
        print(ATM_CALL)
        return ATM_CALL
    elif PE_Return == 'True' and CE_Return=='False':
        print(ATM_PUT)
        return ATM_PUT
    elif PE_Return == 'True' and CE_Return=='True':
        print(ATM_CALL,ATM_PUT)
        return ATM_CALL,ATM_PUT


####################################################################################################
# NEW CODE BELOW (EXISTING CODE ABOVE IS UNCHANGED)
####################################################################################################

instrumentsNfoCache = None
instrumentsNfoCacheDate = None


def GetKiteClient():
    with open(KiteEshitaLogin, "r") as a:
        content = a.readlines()
    apiKey = content[2].strip("\n")
    kite = KiteConnect(api_key=apiKey)

    with open(KiteEshitaLoginAccessToken, "r") as f:
        accessTok = f.read()
    kite.set_access_token(accessTok)
    return kite

def GetInstrumentsNfoCached(kite):
    global instrumentsNfoCache, instrumentsNfoCacheDate
    todayDate = date.today()
    if instrumentsNfoCache is None or instrumentsNfoCacheDate != todayDate:
        instrumentsNfoCache = kite.instruments("NFO")
        instrumentsNfoCacheDate = todayDate
    return instrumentsNfoCache

def ChunkList(items, chunkSize):
    for i in range(0, len(items), chunkSize):
        yield items[i:i + chunkSize]

def GetBestMarketPremium(quoteData, tradeType):
    depth = quoteData.get("depth") or {}
    buys = depth.get("buy") or []
    sells = depth.get("sell") or []
    lastPrice = float(quoteData.get("last_price") or 0.0)

    if tradeType == "SELL":
        if len(buys) > 0 and float(buys[0].get("price") or 0) > 0:
            return float(buys[0]["price"])
        return lastPrice

    if tradeType == "BUY":
        if len(sells) > 0 and float(sells[0].get("price") or 0) > 0:
            return float(sells[0]["price"])
        return lastPrice

    return lastPrice


def InferStrikeStep(instrumentsOpt, indexName, expiryDate, optSegment):
    strikes = sorted({
        float(ins.get("strike") or 0.0)
        for ins in instrumentsOpt
        if ins.get("segment") == optSegment
        and ins.get("name") == indexName
        and ins.get("expiry") == expiryDate
        and float(ins.get("strike") or 0.0) > 0
    })
    if len(strikes) < 3:
        return None
    diffs = [round(strikes[i+1] - strikes[i], 10) for i in range(len(strikes)-1)]
    diffs = [d for d in diffs if d > 0]
    if not diffs:
        return None
    return min(diffs)


instrumentsCacheByExchange = {}
instrumentsCacheDateByExchange = {}

def GetDerivativesExchange(indexName):
    if indexName == "SENSEX":
        return "BFO"
    return "NFO"

def GetOptSegmentForExchange(exchange):
    return f"{exchange}-OPT"

def GetInstrumentsCached(kite, exchange):
    todayDate = date.today()
    if instrumentsCacheByExchange.get(exchange) is None or instrumentsCacheDateByExchange.get(exchange) != todayDate:
        instrumentsCacheByExchange[exchange] = kite.instruments(exchange)
        instrumentsCacheDateByExchange[exchange] = todayDate
    return instrumentsCacheByExchange[exchange]

def GetAvailableExpiryDates(instrumentsOpt, indexName, optSegment):
    expirySet = set()
    for ins in instrumentsOpt:
        if ins.get("segment") != optSegment:
            continue
        if ins.get("name") != indexName:
            continue
        expiryVal = ins.get("expiry")
        if expiryVal is not None:
            expirySet.add(expiryVal)
    return sorted(list(expirySet))

def SelectExpiryDateFromInstruments(instrumentsOpt, indexName, optionType, expiryWeekdayInt, optSegment):
    todayDate = date.today()
    expiryDates = GetAvailableExpiryDates(instrumentsOpt, indexName, optSegment)

    futureExpiries = [e for e in expiryDates if e >= todayDate]
    if len(futureExpiries) == 0:
        raise Exception(f"No future expiries found in instruments for {indexName} ({optSegment})")

    if str(optionType) == "MonthlyOption":
        thisMonth = todayDate.month
        thisYear = todayDate.year

        thisMonthExpiries = [e for e in futureExpiries if e.month == thisMonth and e.year == thisYear]
        if len(thisMonthExpiries) > 0:
            return max(thisMonthExpiries)

        nextMonthDate = todayDate + relativedelta(months=1)
        nextMonthExpiries = [e for e in futureExpiries if e.month == nextMonthDate.month and e.year == nextMonthDate.year]
        if len(nextMonthExpiries) > 0:
            return max(nextMonthExpiries)

        return max(futureExpiries)

    # WeeklyOption (recommended): nearest expiry from instruments (most robust)
    if str(optionType) == "WeeklyOption" or str(optionType) == "" or optionType is None:
        return min(futureExpiries)

    # Optional: weekday filter (fallback)
    targetWeekday = int(expiryWeekdayInt)
    weekdayMatches = [e for e in futureExpiries if e.weekday() == targetWeekday]
    if len(weekdayMatches) > 0:
        return min(weekdayMatches)

    return min(futureExpiries)

def FetchOptionNameByPremium(kite, instrumentsOpt, exchange, orderDetails, targetPremium, strikeWindow=20):
    indexName = orderDetails["Tradingsymbol"]
    tradeType = orderDetails.get("Tradetype", "SELL")
    useAllStrikes = str(orderDetails.get("UseAllStrikes", "False")) == "True"

    optSegment = GetOptSegmentForExchange(exchange)

    expiryDate = SelectExpiryDateFromInstruments(
        instrumentsOpt=instrumentsOpt,
        indexName=indexName,
        optionType=orderDetails.get("OptionType"),
        expiryWeekdayInt=int(orderDetails.get("OptionExpiryDay", 0)),
        optSegment=optSegment
    )

    underlyingMap = {
        "NIFTY": "NSE:NIFTY 50",
        "BANKNIFTY": "NSE:NIFTY BANK",
        "FINNIFTY": "NSE:NIFTY FIN SERVICE",
        "MIDCPNIFTY": "NSE:NIFTY MID SELECT",
        "SENSEX": "BSE:SENSEX"
    }

    strikeStepMap = {
        "NIFTY": 50,
        "BANKNIFTY": 100,
        "FINNIFTY": 50,
        "MIDCPNIFTY": 25,
        "SENSEX": 100
    }

    underlyingKey = underlyingMap[indexName]
    spot = float(kite.ltp(underlyingKey)[underlyingKey]["last_price"])

    inferred = InferStrikeStep(instrumentsOpt, indexName, expiryDate, optSegment)
    strikeStep = int(inferred) if inferred else int(strikeStepMap.get(indexName, 50))

    atmStrike = int(round(spot / strikeStep) * strikeStep)

    minStrike = atmStrike - (int(strikeWindow) * strikeStep)
    maxStrike = atmStrike + (int(strikeWindow) * strikeStep)

    optionSide = "CE" if str(orderDetails.get("CallStrikeRequired")) == "True" else "PE"

    candidates = []
    for ins in instrumentsOpt:
        if ins.get("segment") != optSegment:
            continue
        if ins.get("name") != indexName:
            continue
        if ins.get("expiry") != expiryDate:
            continue
        if ins.get("instrument_type") != optionSide:
            continue

        strike = float(ins.get("strike") or 0.0)
        if not useAllStrikes:
            if strike < minStrike or strike > maxStrike:
                continue

        candidates.append(ins)

    if len(candidates) == 0:
        raise Exception(f"No candidates found for {indexName} {expiryDate} {optionSide} ({exchange})")

    bestSymbol = None
    bestDiff = float("inf")

    quoteKeys = [f"{exchange}:{c['tradingsymbol']}" for c in candidates]

    for chunk in ChunkList(quoteKeys, 150):
        quotes = kite.quote(chunk)
        time.sleep(0.2)

        for qk in chunk:
            q = quotes.get(qk)
            if not q:
                continue

            premium = GetBestMarketPremium(q, tradeType)
            if premium <= 0:
                continue

            diff = abs(premium - float(targetPremium))
            if diff < bestDiff:
                bestDiff = diff
                bestSymbol = qk.split(":", 1)[1]

    if not bestSymbol:
        raise Exception("Could not select a premium-based contract (illiquid / empty quotes).")

    return bestSymbol

def FetchContractName(orderDetails):
    fetchByPremium = str(orderDetails.get("FetchByPremium", "")).strip()
    if fetchByPremium != "" and fetchByPremium.lower() not in {"false", "none"}:
        kite = GetKiteClient()

        indexName = orderDetails["Tradingsymbol"]
        exchange = GetDerivativesExchange(indexName)
        instrumentsOpt = GetInstrumentsCached(kite, exchange)

        targetPremium = float(fetchByPremium)
        strikeWindow = int(orderDetails.get("StrikeWindow", 20))

        ceReq = str(orderDetails.get("CallStrikeRequired")) == "True"
        peReq = str(orderDetails.get("PutStrikeRequired")) == "True"

        if ceReq and peReq:
            ceDetails = dict(orderDetails)
            peDetails = dict(orderDetails)

            ceDetails["CallStrikeRequired"] = "True"
            ceDetails["PutStrikeRequired"] = "False"

            peDetails["CallStrikeRequired"] = "False"
            peDetails["PutStrikeRequired"] = "True"

            ceSymbol = FetchOptionNameByPremium(kite, instrumentsOpt, exchange, ceDetails, targetPremium, strikeWindow=strikeWindow)
            peSymbol = FetchOptionNameByPremium(kite, instrumentsOpt, exchange, peDetails, targetPremium, strikeWindow=strikeWindow)
            return ceSymbol, peSymbol

        return FetchOptionNameByPremium(kite, instrumentsOpt, exchange, orderDetails, targetPremium, strikeWindow=strikeWindow)

    return FetchOptionName(orderDetails)

if __name__ == '__main__':

    niftyOrderDetails = {
        "Tradingsymbol": "NIFTY",
        "OptionContractStrikeFromATMPercent": 0,
        "Hedge": "False",
        "CallStrikeRequired": "True",
        "PutStrikeRequired": "False",
        "OptionType": "WeeklyOption",
        "OptionExpiryDay": "1",      # keep if you want; with WeeklyOption we pick nearest expiry anyway
        "FetchByPremium": "250",
        "StrikeWindow": "25",
        "Tradetype": "SELL"
    }

    sensexOrderDetails = {
        "Tradingsymbol": "SENSEX",
        "OptionContractStrikeFromATMPercent": 0,
        "Hedge": "False",
        "CallStrikeRequired": "True",
        "PutStrikeRequired": "False",
        "OptionType": "WeeklyOption",
        "OptionExpiryDay": "3",
        "FetchByPremium": "250",
        "StrikeWindow": "25",
        "Tradetype": "SELL"
    }

    k = FetchContractName(niftyOrderDetails)
    print("SelectedContract:", k)