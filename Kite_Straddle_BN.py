import logging
from os import abort
import string
from kiteconnect import KiteConnect
from datetime import datetime,timedelta
from datetime import date

logging.basicConfig(level=logging.DEBUG)

with open('C:/Users/ekans/Documents/Kite_API/inputs/api_key.txt','r') as a:
        api_key = a.read()
        a.close()
kite = KiteConnect(api_key=api_key)


with open('C:/Users/ekans/Documents/Kite_API/inputs/access_token.txt','r') as f:
    access_tok = f.read()
    f.close()
    #print(access_tok)
kite.set_access_token(access_tok)
one_shot_flag = True

##################################################################################################################################
#Get the date of this year ,month and day

Thursday_date = date.today()
while Thursday_date.weekday() != 3:# weekday() can be used to retrieve the day of the week. The datetime.today() method returns the current date, and the weekday() method returns the day of the week as an integer where Monday is indexed as 0 and Sunday is 6.
    Thursday_date += timedelta(1)  #Since the options expire on thursday their namefield will have the corresponding date field of that day
y0= Thursday_date.strftime("%y")#if the last thursday comes in next year then the year will be rolled over
year = int(y0)

#Get the month for the option contract
Thursday_date = date.today()
while Thursday_date.weekday() != 3:
    Thursday_date += timedelta(1)  
m0= Thursday_date.strftime("%m")#find out using the days left to next month, if the next month value needs to be added. This can happen during the last week of the month when the next month's weekly contract needs to selected.
month=int(m0)#month has to be converted into an integer because it cannot have 0 suffixing it if it is a single digit month eg in the contract 

#get the date of next thursday
Thursday_date = date.today()
while Thursday_date.weekday() != 3:
    Thursday_date += timedelta(1)
    d0= Thursday_date.strftime("%d")
    day=d0
if date.today().weekday() ==3: 
    day= date.today().strftime("%d")#needs to be in string format as it has to be passed in %d%d format , i.e 3rd March will go as '03'/3

#Default time the order must be placed
sec = str("00")
min = str("15")
hr = str("11")
##################################################################################################################################
#Order Inputs
Quantity = 25
order_type = kite.ORDER_TYPE_LIMIT #'kite.ORDER_TYPE_MARKET'

order_exchange = kite.EXCHANGE_NFO

order_variety = kite.VARIETY_AMO #'kite.VARIETY_REGULAR'

order_product = kite.PRODUCT_MIS #kite.PRODUCT_NRML

order_buy = kite.TRANSACTION_TYPE_BUY

order_sell = kite.TRANSACTION_TYPE_SELL

order_validity = kite.VALIDITY_DAY

hedge_percent = int(7)

print(" "*10+"Verify the following parameters for the order to be placed. G--Go Ahead!  N-->Abort the execution  M-->Modify any of the parameters")


print("The option series that will be traded is" +" "+"BANKNIFTY"+str(year)+str(month)+str(day)+"*")
print("Time at which the option trade will be executed" + " "*5 +str(hr)+":"+str(min)+":"+str(sec))
print("Order parameters are :"+ order_type+" "+ order_exchange+" "+order_variety+" "+order_product+" "+order_validity)
print("The positions will be hedged by "+str(hedge_percent)+"%"+" "+"OTM options")

proceed = input()
if proceed in {"G","g"}:    #{} is a set
    y=year
    m=month
    d=day
    


if proceed in {"M","m"}:

    y = input("Enter the year of the option contract you intend to place order in --Format in %y%y") or year
    m = input("Enter the month of the option contract you intend to place order in --Format in %m") or month
    d = input("Enter the day of the option contract you intend to place order in --Format in %d%d") or day
    #################################################################################################################################
    #Get the time at which you want the order to be placed
    hedge_percent = int(input("Enter how far OTM percent short calls should be hedged by")) or int(7)

    hr = input("Hour at which the order should be routed") or datetime.now().hour
    min = input("Minute at which the order should be routed") or datetime.now().minute
    sec = input("Second at which the order should be routed") or datetime.now().second
if proceed in {"N","n"}:
    abort()

while one_shot_flag == True:
    ##################################################################################################################################
    #Find the current value of the bank nifty index
    Banknifty_index = {260105:'NIFTY BANK'}

    for val in Banknifty_index:
        price = kite.ltp('NSE:' + Banknifty_index[val])#this will send ohlc price in dictionary format
        #print(price)
        ltp = price['NSE:'+Banknifty_index[val]]['last_price']#to get ltp of whichever stick is declared in token
        print("BankNifty LTP:"+str(ltp))

    ##################################################################################################################################
    #Get the ATM contract
    diff = ltp%100
    #print(diff)

    '''if diff >=50:
        ATM_ltp = int(ltp+ (100-diff))
    if diff<50:
        ATM_ltp = int(ltp - diff)'''
    ATM_ltp = int(round(ltp,-2))#This will round to the nearest hundrend place so that we can select the nearst ATM contract    

    #print("ATM Contract is:"+ATM_ltp)

    #Calculate the proper hedging value for the ATM option
    r = ATM_ltp#to round to the nearest 10,000
    ATM_HEDGE_CE = round(int(r*((100+hedge_percent)/100) + ATM_ltp%1000),-2)#-2 to round it to the nearest hundreds place(since that is the steps in which options are priced)
    #print(ATM_HEDGE_CE)

    ATM_HEDGE_PE = round(int(r*((100-hedge_percent)/100) - ATM_ltp%1000),-2)
    #print(ATM_HEDGE_PE)
    #print("Hedging the short call by"+ATM_HEDGE_CE)
    #print("Hedging the short put by"+ATM_HEDGE_PE)
    ##################################################################################################################################
    #Append the dates and the script name to create the complete contract names to be Traded
    ATM_CALL = 'BANKNIFTY'+str(y)+str(m)+str(d)+str(ATM_ltp)+'CE'
    ATM_HEDGE_CALL = 'BANKNIFTY'+str(y)+str(m)+str(d)+str(ATM_HEDGE_CE)+'CE'

    ATM_PUT = 'BANKNIFTY'+str(y)+str(m)+str(d)+str(ATM_ltp)+'PE'
    ATM_HEDGE_PUT = 'BANKNIFTY'+str(y)+str(m)+str(d)+str(ATM_HEDGE_PE)+'PE'
    print("The ATM CALL contract is:"+ATM_CALL)
    print("The ATM PUT contract is:"+ATM_PUT)
    print("Hedging the short call by"+ATM_HEDGE_CALL)
    print("Hedging the short put by"+ATM_HEDGE_PUT)
    ##################################################################################################################################
    #Fetch the tokens from the csv file

   

    #while True:
    if (datetime.now().second == int(sec)) and (datetime.now().minute == int(min)) and (datetime.now().hour == int(hr)) and (one_shot_flag == True): 
                                                                                                       #must be FRIDAY

                one_shot_flag = False
                sell_call = kite.place_order(variety= order_variety,
                                                                exchange=order_exchange,
                                                                order_type=order_type,
                                                                tradingsymbol=ATM_CALL,
                                                                transaction_type=order_sell,
                                                                quantity=Quantity,
                                                                validity=order_validity,
                                                                product=order_product,
                                                                price = 1239.00)

                hedge_call = kite.place_order(variety= order_variety,
                                                                exchange=order_exchange,
                                                                order_type=order_type,
                                                                tradingsymbol=ATM_HEDGE_CALL,
                                                                transaction_type=order_buy,
                                                                quantity=Quantity,
                                                                validity=order_validity,
                                                                product=order_product,
                                                                price = 4)  
                sell_put = kite.place_order(variety= order_variety,
                                                                exchange=order_exchange,
                                                                order_type=order_type,
                                                                tradingsymbol=ATM_PUT,
                                                                transaction_type=order_sell,
                                                                quantity=Quantity,
                                                                validity=order_validity,
                                                                product=order_product,
                                                                price = 1039.00)

                hedge_put = kite.place_order(variety= order_variety,
                                                                exchange=order_exchange,
                                                                order_type=order_type,
                                                                tradingsymbol=ATM_HEDGE_PUT,
                                                                transaction_type=order_buy,
                                                                quantity=Quantity,
                                                                validity=order_validity,
                                                                product=order_product,
                                                                price = 4)

        
                                                                                                                                                                                              
print(kite.orders())
