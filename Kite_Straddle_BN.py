from calendar import THURSDAY
import logging
from os import abort
import string
from kiteconnect import KiteConnect
from datetime import datetime,timedelta
from datetime import date
from Set_Gtt_Exit import Set_Gtt
from inputimeout import inputimeout,TimeoutOccurred
from dateutil.relativedelta import TH, relativedelta
import time

logging.basicConfig(level=logging.DEBUG)
y = ''
m = ''
d = ''


with open('C:/Users/ekans/Documents/inputs/api_key_IK.txt','r') as a:
        api_key = a.read()
        a.close()
kite = KiteConnect(api_key=api_key)


with open('C:/Users/ekans/Documents/inputs/access_token_IK.txt','r') as f:
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
#m0= Thursday_date.strftime("%m")#No longeer in use as the format has been changed #find out using the days left to next month, if the next month value needs to be added. This can happen during the last week of the month when the next month's weekly contract needs to selected.
m0 = (Thursday_date.strftime("%b")).upper()#Fetch the name of the month of the option series to be executed
d0= Thursday_date.strftime("%d")

year = int(y0)
#month=int(m0)#month has to be converted into an integer because it cannot have 0 suffixing it if it is a single digit month eg in the contract 
month = m0[0]#Get the first letter of the month for new format
day=d0

if date.today().weekday() ==3: 
    day= date.today().strftime("%d")#needs to be in string format as it has to be passed in %d%d format , i.e 3rd March will go as '03'/3

#Fetch the last thursday of the month
last_day = (date.today()+relativedelta(day=31, weekday=TH(-1)))

#Check if the day is last Thursday of the month (Since Thursday is the expiry day for options)
if (Thursday_date == last_day):
    #If true then need to change the value for month and day as the name format of the option contract(Month expiry Option ) changes
    month=(date.today().strftime("%b")).upper()
    day=''

#Default time the order must be placed
sec = str("00")
min = str("15")
hr = str("11")
##################################################################################################################################
#Order Inputs
Quantity = 25
order_type = kite.ORDER_TYPE_MARKET

order_exchange = kite.EXCHANGE_NFO

order_variety =kite.VARIETY_REGULAR

order_product = kite.PRODUCT_NRML

order_buy = kite.TRANSACTION_TYPE_BUY

order_sell = kite.TRANSACTION_TYPE_SELL

order_validity = kite.VALIDITY_DAY

hedge_percent = int(9)

#To First Show up on console
print(" "*10+"Verify the following parameters for the order to be placed. G--Go Ahead!  N-->Abort the execution  M-->Modify any of the parameters")


print("The option series that will be traded is" +" "+"BANKNIFTY"+str(year)+str(month)+str(day)+"*")
print("Time at which the option trade will be executed" + " "*5 +str(hr)+":"+str(min)+":"+str(sec))
print("Order parameters are :"+ order_type+" "+ order_exchange+" "+order_variety+" "+order_product+" "+order_validity)
#print("The positions will be hedged by "+str(hedge_percent)+"%"+" "+"OTM options")
print("Quantity:"+str(Quantity))

#Give multiple options to execute
try:
    proceed = inputimeout(timeout=5)
    if proceed in {"G","g"}:    #{} is a set
        y=year
        m=month
        d=day
        
    if proceed in {"M","m"}:

        y = input("Enter the year of the option contract you intend to place order in --Format in %y%y") or year
        m = input("Enter the month of the option contract you intend to place order in --Format in %m") or month
        d = input("Enter the day of the option contract you intend to place order in --Format in %d%d (DEFAULT-->NULL)") or ''
        #################################################################################################################################
        #Get the time at which you want the order to be placed
        #hedge_percent = input("Enter how far OTM percent short calls should be hedged by") or int(7)

        hr = input("Hour at which the order should be routed") or datetime.now().hour
        min = input("Minute at which the order should be routed") or datetime.now().minute
        sec = input("Second at which the order should be routed") or datetime.now().second
    if proceed in {"N","n"}:
        abort()

#In case of timeout then the script will execute with the default values    
except TimeoutOccurred:
        y=year
        m=month
        d=day

while one_shot_flag == True:

    ##################################################################################################################################
    #Find the current value of the bank nifty index
    Banknifty_index = {260105:'NIFTY BANK'}
    
    for val in Banknifty_index:
        time.sleep(0.1)
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
    ATM_HEDGE_CE = round(int(r*((100+int(hedge_percent))/100)),-2)#-2 to round it to the nearest hundreds place(since that is the steps in which options are priced)
    #print(ATM_HEDGE_CE)

    ATM_HEDGE_PE = round(int(r*((100-int(hedge_percent))/100)),-2)
    #print(ATM_HEDGE_PE)
    #print("Hedging the short call by"+ATM_HEDGE_CE)
    #print("Hedging the short put by"+ATM_HEDGE_PE)
    ##################################################################################################################################
    #Append the dates and the script name to create the complete contract names to be Traded
    ATM_CALL = 'BANKNIFTY'+str(y)+str(m)+str(d)+str(ATM_ltp)+'CE'
    ATM_HEDGE_CALL = 'BANKNIFTY'+str(y)+str(m)+str(d)+str(ATM_HEDGE_CE)+'CE'

    ATM_PUT = 'BANKNIFTY'+str(y)+str(m)+str(d)+str(ATM_ltp)+'PE'
    ATM_HEDGE_PUT = 'BANKNIFTY'+str(y)+str(m)+str(d)+str(ATM_HEDGE_PE)+'PE'
    print("The ATM CALL contract format is:"+ATM_CALL)
    print("The ATM PUT contract format is:"+ATM_PUT)
    #print("Hedging the short call by"+ATM_HEDGE_CALL)
    #print("Hedging the short put by"+ATM_HEDGE_PUT)
    ##################################################################################################################################
    #Fetch the tokens from the csv file

   

    #while True:
    if (datetime.now().second == int(sec)) and (datetime.now().minute == int(min)) and (datetime.now().hour == int(hr)) and (one_shot_flag == True): 
                                                                                                       #must be FRIDAY
                #Set to false to ensure that the trade is executed only once
                one_shot_flag = False
                sell_call = kite.place_order(variety= order_variety,
                                                                exchange=order_exchange,
                                                                order_type=order_type,
                                                                tradingsymbol=ATM_CALL,
                                                                transaction_type=order_sell,
                                                                quantity=Quantity,
                                                                validity=order_validity,
                                                                product=order_product
                                                                )
                #Set GTT for the stoploss amount                                                
                Set_Gtt(ATM_CALL,Quantity)
                '''hedge_call = kite.place_order(variety= order_variety,
                                                                exchange=order_exchange,
                                                                order_type=order_type,
                                                                tradingsymbol=ATM_HEDGE_CALL,
                                                                transaction_type=order_buy,
                                                                quantity=Quantity,
                                                                validity=order_validity,
                                                                product=order_product
                                                                )'''
                sell_put = kite.place_order(variety= order_variety,
                                                                exchange=order_exchange,
                                                                order_type=order_type,
                                                                tradingsymbol=ATM_PUT,
                                                                transaction_type=order_sell,
                                                                quantity=Quantity,
                                                                validity=order_validity,
                                                                product=order_product
                                                                )
                Set_Gtt(ATM_PUT,Quantity)
                '''hedge_put = kite.place_order(variety= order_variety,
                                                                exchange=order_exchange,
                                                                order_type=order_type,
                                                                tradingsymbol=ATM_HEDGE_PUT,
                                                                transaction_type=order_buy,
                                                                quantity=Quantity,
                                                                validity=order_validity,
                                                                product=order_product
                                                                )'''

        
                                                                                                                                                                                              
print(kite.orders())
