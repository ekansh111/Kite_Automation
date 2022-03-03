from calendar import TUESDAY
from datetime import date, datetime,timedelta
from datetime import datetime
from decimal import ROUND_UP
import math
from typing import Type
from dateutil.relativedelta import TH, relativedelta
import datetime
import pyotp
totp = pyotp.TOTP('ABCD')
#print(totp.now())
'''tokens = {738561:'RELIANCE'}
for tokn in tokens:
    print(tokn)'''


'''ltp = 38549
x = ltp%100
print(x)
if x >=50:
    ltp = ltp+ (100-x)
if x<50:
    ltp = ltp - (100+x)

print(ltp)
ltp = 38549
diff = ltp%100
print(diff)
if diff >=50:
    ATM_ltp = ltp+ (100-diff)
if diff<50:
    ATM_ltp = ltp -diff

print(ATM_ltp)'''
'''date_ = date.today()
y= date_.strftime("%y")
print(y)'''


'''date_ = date.today()
m0= date_.strftime("%m")
m=abs(int(m0))
print(m)
#print(m0)

#print(datetime[year])'''
'''date_ = date.today()
d= date_.strftime("%d")
print(d)'''
#print(m0)

#print(datetime[year])

'''r = round(ATM_ltp,-4)
hedge_percent = 7
ATM_HEDGE = int(r*((100+hedge_percent)/100))
print(ATM_HEDGE)'''

'''k = datetime.now()
print(type(k))


print(datetime.now())'''


'''Thursday_date = date.today()
while Thursday_date.weekday() != 3:# weekday() can be used to retrieve the day of the week. The datetime.today() method returns the current date, and the weekday() method returns the day of the week as an integer where Monday is indexed as 0 and Sunday is 6.
    Thursday_date += timedelta(1)
    d0= Thursday_date.strftime("%d")'''

'''Thursday_date = date(2022,12,31)
while Thursday_date.weekday() != 3:# weekday() can be used to retrieve the day of the week. The datetime.today() method returns the current date, and the weekday() method returns the day of the week as an integer where Monday is indexed as 0 and Sunday is 6.
    Thursday_date += timedelta(1)  #Since the options expire on thursday their namefield will have the corresponding date field of that day
m0= Thursday_date.strftime("%y")#find out using the days left to next month, if the next month value needs to be added. This can happen during the last week of the month when the next month's weekly contract needs to selected.
month=abs(int(m0))

print(month)'
#print(date.today())'''

#print(round(38929,-2))
'''hedge_percent = 6
ATM_ltp = 38900
ATM_HEDGE_CE = round(int(ATM_ltp*((100+hedge_percent)/100) + (ATM_ltp%1000)),-2)
print(ATM_HEDGE_CE)

print(ATM_ltp%100)'''
'''n = 38973
def round_half_away_from_zero(n, decimals=1):
    rounded_abs = round_half_up(abs(n), decimals)
    return math.copysign(rounded_abs, n)'''

'''m = "kite.EXCHANGE_NSE"
kk = (m[1:-1])
print(kk'''

'''dict = {"tradetype":print}
print(dict["tradetype"])'''
#print(round(937.8,-3))

#y = datetime.date.today()+relativedelta(day=31, weekday=TH(1))
#print(y)
#datetime.date(2021, 6, 25)

'''k = (datetime.date(2022,7,1)+relativedelta(day=31, weekday=TH(-1)))
d = k.strftime("%d")
m = k.strftime("%B")
y = k.strftime("%y")

print(d)
print(m)
print(y)
print(k)


atm_ltp = 37300
print(atm_ltp%1000)


Token = {260105:'NIFTY BANK'}
for val in Token:
    print(Token[val])'''

with open('C:/Users/ekans/Documents/inputs/Login_Credentials_IK.txt','r') as a:
        content = a.readlines()
        a.close()

user_id= content[0].strip('\n')
user_pwd = content[1].strip('\n')
api_key = content[2].strip('\n')
api_secret = content[3].strip('\n')
totp_key= content[4].strip('\n')

print(user_id)
print(user_pwd)
print(api_key)
print(api_secret)
print(totp_key)

with open('C:/Users/ekans/Documents/inputs/api_key_IK.txt','r') as a:
        api_key = a.read()
        a.close()
with open('C:/Users/ekans/Documents/inputs/api_secret_IK.txt','r') as a:
        api_secret = a.read()
        a.close()
with open('C:/Users/ekans/Documents/inputs/user_id_IK.txt','r') as a:
        user_id = a.read()
        a.close()
with open('C:/Users/ekans/Documents/inputs/user_pwd_IK.txt','r') as a:
        user_pwd = a.read()
        a.close()
with open('C:/Users/ekans/Documents/inputs/totp_key_IK.txt','r') as a:
        totp_key = a.read()
        a.close()


print(api_key)
print(api_secret)
print(user_id)
print(user_pwd)
print(totp_key)