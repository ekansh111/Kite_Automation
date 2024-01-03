# package import statement
from SmartApi import SmartConnect
import SmartApi #or from SmartApi.smartConnect import SmartConnect
import pyotp
import time
from datetime import date, datetime
import calendar
import pytz
from Directories import *

#Types of Orders
LimitOrder = 'LIMIT'
MarketOrder = 'MARKET'
#Limit Order wait time in seconds
LimitOrderWaitTime = 120

#Function to handle disreparency in quantity and lotsizes for order to be placed
def Validate_Quantity(order_details_fetch):

    Quantitysplit = str(order_details_fetch['Quantity']).split('*')

    #If there is any disreparency between the total quantity and lotsize the correct it
    if len(Quantitysplit)>1:
        UpdatedQuantity = int(Quantitysplit[0]) * int(Quantitysplit[1])
        order_details_fetch['Quantity'] = UpdatedQuantity 
        print(UpdatedQuantity)
    
    return True


#Function to validate the month for the cutoff date post which the next month contract should be used
def Validate_Month(smartApi,order_details_fetch):
    #Fetch the symbol name
    OrderPlacedContract = str(order_details_fetch['Tradingsymbol']).replace(" ","").upper()
    #Fetch the current month and date
    currentmonth=(date.today().strftime("%b")).upper()
    currentdate=(date.today().strftime("%d")).upper()

    #Fetching the contract month name for NCDEX format futures
    if str(order_details_fetch['Exchange']) == 'NCDEX':
        #Month Name for NCDEX contract is named in the below format
        OrderPlaceContractMonth = OrderPlacedContract[-7:-4]
    else:
        return True

    #Update the contract name and replace the order, net position no longer considered as it could lead to a 
    #situation where entry is placed in next month contract and then while exiting the cureent month contract order is 
    #placed, which will lead to 2 open contracts of different months
    if ((OrderPlaceContractMonth == currentmonth) and (int(currentdate) >5)): #and (order_details_fetch.get("Netposition") != '0')):

        #Holds the current month sequence number 
        CurrentMonthSeq = date.today().month
        #Next month sequence number
        UpdatedMonthSeq=CurrentMonthSeq + 1
        #Fetch the month name from the sequence number
        UpdatedMonthName = calendar.month_abbr[UpdatedMonthSeq].upper()

        #The contract year is the last 4 characters
        ContractYear = OrderPlacedContract[-4:]
        #Replace redundant details from ttthe contract
        Replace = OrderPlacedContract[-7:]

        UpdatedContractName = OrderPlacedContract.replace(Replace,'') + UpdatedMonthName + ContractYear

        #For NCDEX contracts the Trading Symbol and the symbol token are generally the same
        order_details_fetch['Tradingsymbol'] = UpdatedContractName
        order_details_fetch['Symboltoken'] = UpdatedContractName

        print('Updated Contract Name --> ' + str(UpdatedContractName))
        #To not retry placing the order as market, since it will be risky considering the contract name is manually updated
        order_details_fetch['MarketRetryFlag'] = 0
        Limit_Order_Type(smartApi,order_details_fetch)
    else:
        return True



#Function to place market order if the limit order failed
def Market_Order_Type(smartApi,order_details_fetch):
    
    order_details_fetch['Price'] = '0'
    order_details_fetch['Ordertype'] = MarketOrder
    #Identify that the market order is placed, so to not call this function again
    order_details_fetch['MarketRetryFlag'] = False

    Angel_Order_place(smartApi,order_details_fetch)

#Function to place Limit order first then if not filled , re-place Market Order
def Limit_Order_Type(smartApi,order_details_fetch):

    exchange = str(order_details_fetch['Exchange'])
    tradingsymbol = str(order_details_fetch['Tradingsymbol']).replace(" ","")
    symboltoken = str(order_details_fetch['Symboltoken'])
    #print(exchange,tradingsymbol,symboltoken)
    #Fetch Instrument LTP
    LtpInfo = smartApi.ltpData(exchange=exchange,tradingsymbol=tradingsymbol,symboltoken=symboltoken)
    
    Instrumentdata = LtpInfo['data']
    #print(Instrumentdata['ltp'])
    
    order_details_fetch['Price'] = Instrumentdata['ltp']
    order_details_fetch['Ordertype'] = LimitOrder

    Angel_Order_place(smartApi,order_details_fetch)

#Function to place order on Angel Broking account
def Angel_Order_place(smartApi,order_details_fetch):

    #place order
    try:

        #Prepare the request for placing the order through API
        orderparams = {
            "variety":str(order_details_fetch['Variety']),#Kind of order AMO/NORMAL ...   
            "tradingsymbol":str(order_details_fetch['Tradingsymbol']).replace(" ","").upper(),#The intrument name
            "symboltoken":str(order_details_fetch['Symboltoken']),#Symbol token
            "transactiontype":str(order_details_fetch['Tradetype']).upper(),#Buy/Sell
            "exchange":str(order_details_fetch['Exchange']),#Exchange to place the order on
            "ordertype":str(order_details_fetch['Ordertype']),#LIMIT/MARKET.. Order
            "producttype":str(order_details_fetch['Product']),#CARRYFORWARD for futures
            "duration":str(order_details_fetch['Validity']),#DAY
            "price":str(order_details_fetch['Price']) or "0",
            "squareoff":str(order_details_fetch['Squareoff']) or "0",
            "stoploss":str(order_details_fetch['Stoploss']) or "0",
            "quantity":str(order_details_fetch['Quantity'])#Quantity according to angel one multiplier set
            }
        
        #print(orderparams)
        orderId=smartApi.placeOrder(orderparams)
    except Exception as e:
        print("Order placement failed: {}".format(e.message))

    print("The order id is: {}".format(orderId))

    #Wait for a set amount of time for the order to be filled, if limit order is still not placed then go market order
    time.sleep(LimitOrderWaitTime)

    #Place order cancellation after the specified wait time, if the order was placed successfully then the cancellation will fail
    cancel = smartApi.cancelOrder(orderId,str(order_details_fetch['Variety']))
    
    #Sleep for 3 seconds to let the Tradebook get updated
    time.sleep(3)

    #Fetch the Tradebook for all the trades placed today, To check if the cancelled order was already successfully placed or not.
    TradeBook = smartApi.tradeBook()

    if TradeBook['data'] != None:
        #Hold the number of trades that were placed upto that trading day, -1 for adjusting for indexation from 0
        #Add a check here to handle if no trades were placed in entire day 
        LengthTrades = len(TradeBook['data']) -1
        #orderId = 230905001463384
        #Loop through the order details for all orders placed on the da. TradeBook['data'] is an array of hash of orders
        for orderlist in TradeBook['data'] :

            #Fetch the order id for all the orders of the day
            orderlistidhistory = TradeBook['data'][LengthTrades]['orderid']
            #Loop through trade list
            LengthTrades = LengthTrades-1

            #If the current order has been successfully placed , then set the Market retry flag to false, and the order should not be placed
            if str(orderId) == str(orderlistidhistory):
                order_details_fetch['MarketRetryFlag'] = False
                print('The order ' + str(orderlistidhistory) + 'has already been placed')
                #EXIT HERE ITSELF
                return True
                #exit(1)
                #print(orderId)

    #If cancellation is successfull then place market order(If the Market Retry flag is not populated with any value)
    if((str(cancel['status']) == 'True') and (str(cancel['message']) == 'SUCCESS') and (order_details_fetch.get("MarketRetryFlag") is None)):
        print("Order cancelled placing market order" + str(cancel))
        #PLace market order
        Market_Order_Type(smartApi,order_details_fetch) 


#Function to establish a connection with the API
def Login_Angel_Api(order_details_fetch):
    #print(order_details_fetch)
    Directory = AngelEkanshLoginCred
    if str(order_details_fetch.get('User')) == 'nararush':
        Directory = AngelNararushLoginCred      
    with open(Directory,'r') as a:
        content = a.readlines()
        a.close()
    #print(content)    
    api_key = content[0].strip('\n')
    clientId = content[1].strip('\n')
    pwd = content[2].strip('\n')
    smartApi = SmartConnect(api_key)
    token = content[3].strip('\n')
    totp=pyotp.TOTP(token).now()
    correlation_id = "abc123"

    # login api call
    data = smartApi.generateSession(clientId, pwd, totp)

    # print(data)
    authToken = data['data']['jwtToken']
    refreshToken = data['data']['refreshToken']

    # fetch the feedtoken
    feedToken = smartApi.getfeedToken()

    # fetch User Profile
    res = smartApi.getProfile(refreshToken)
    smartApi.generateToken(refreshToken)
    res=res['data']['exchanges']
    #print(res)
    if (str(order_details_fetch.get('User')) == 'nararush') or (str(order_details_fetch.get('User')) == 'ekansh'):
        #Limit_Order_Type(smartApi,order_details_fetch)
        return smartApi
    else:
        Validate_Order_Details(smartApi,order_details_fetch)

def Validate_Order_Details(smartApi,order_details_fetch):
    #Validate quantity for disreparency between lot size and quantity
    Validate_Quantity(order_details_fetch)

    #Validate the Month of the contract
    Validate_Month(smartApi,order_details_fetch)

    # Get the timezone object for New York
    tz_KT = pytz.timezone('Asia/Kolkata') 

    # Get the current time in Kolkata
    datetime_KT = datetime.now(tz_KT).hour

    #Place Limit order first, unlessn the net postiton is 0, i.e the position is being exited and after 11 am
    if (order_details_fetch.get("Netposition") == '0') and int(datetime_KT)>=11:
        Market_Order_Type(smartApi,order_details_fetch)
    else:    
        Limit_Order_Type(smartApi,order_details_fetch)


if __name__ == '__main__':

    #k = {'Tradetype': 'BUY', 'Exchange': 'NFO', 'Tradingsymbol': 'BANKNIFTY31AUG23FUT', 'Quantity': '15', 'Variety': 'AMO', 'Ordertype': 'LIMIT', 'Product': 'CARRYFORWARD', 'Validity': 'DAY', 'Price': '45017', 'Symboltoken':'35014', 'Squareoff':'', 'Stoploss':''}
    m = {'Tradetype': 'BUY', 'Exchange': 'NCDEX', 'Tradingsymbol': 'CASTOR20OCT2023', 'Quantity': '1*5', 'Variety': 'AMO', 'Ordertype': 'LIMIT', 'Product': 'CARRYFORWARD', 'Validity': 'DAY', 'Price': '45017', 'Symboltoken':'CASTOR20OCT2023', 'Squareoff':'', 'Stoploss':''}

    Login_Angel_Api(m)