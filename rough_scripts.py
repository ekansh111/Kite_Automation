'''from calendar import TUESDAY
from datetime import date, datetime,timedelta
from datetime import datetime
from decimal import ROUND_UP
import math
from multiprocessing.connection import wait
from typing import Type
from dateutil.relativedelta import TH, relativedelta
import datetime
import pyotp
totp = pyotp.TOTP('ABCD')
#print(totp.now())'''
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

'''with open('C:/Users/ekans/Documents/inputs/Login_Credentials_IK.txt','r') as a:
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
print(totp_key)'''

'''from kiteconnect import KiteConnect
from kiteconnect import KiteTicker
import undetected_chromedriver as uc
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
import time, pyotp

options = uc.ChromeOptions()
options.add_argument('--headless')

with open('C:/Users/ekans/Documents/inputs/Login_Credentials_IK.txt','r') as a:
        content = a.readlines()
        a.close()

user_id= content[0].strip('\n')
user_pwd = content[1].strip('\n')
api_key = content[2].strip('\n')
api_secret = content[3].strip('\n')
totp_key= content[4].strip('\n')

def login_in_zerodha(api_key, api_secret, user_id, user_pwd, totp_key):

    
    try:
        driver = uc.Chrome(version_main=98,options=options)
        driver.get(f'https://kite.trade/connect/login?api_key={api_key}&v=3')
        login_id = WebDriverWait(driver, 10).until(lambda x: x.find_element_by_xpath('//*[@id="userid"]'))
        login_id.send_keys(user_id)

        pwd = WebDriverWait(driver, 10).until(lambda x: x.find_element_by_xpath('//*[@id="password"]'))
        pwd.send_keys(user_pwd)

        submit = WebDriverWait(driver, 10).until(lambda x: x.find_element_by_xpath('//*[@id="container"]/div/div/div[2]/form/div[4]/button'))
        submit.click()

        time.sleep(1)
        #adjustment to code to include totp
        totp = WebDriverWait(driver, 10).until(lambda x: x.find_element_by_xpath('//*[@id="totp"]'))
        authkey = pyotp.TOTP(totp_key)
        totp.send_keys(authkey.now())
        #adjustment complete

        continue_btn = WebDriverWait(driver, 10).until(lambda x: x.find_element_by_xpath('//*[@id="container"]/div/div/div[2]/form/div[3]/button'))
        continue_btn.click()
        #print(driver.current_url)
        time.sleep(3)
        #print(driver.current_url)
        url = driver.current_url
        initial_token = url.split('request_token=')[1]
        request_token = initial_token.split('&')[0]
        

        driver.close()

        kite = KiteConnect(api_key = api_key)
        #print(request_token)
        #print(api_secret)
        data = kite.generate_session(request_token,api_secret)
        print(data['access_token'])
        token = data["access_token"] 
        with open('C:/Users/ekans/Documents/inputs/access_token_IK.txt','w') as f:
            f.write(token)
            f.close()

        return kite

    except:
        print('fail')

if __name__ == '__main__':
    login_in_zerodha(api_key, api_secret, user_id, user_pwd, totp_key)'''




'''import time
from click import option
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import undetected_chromedriver as uc
from webdriver_manager.chrome import ChromeDriverManager
from datetime import date
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.chrome.options import Options

options = uc.ChromeOptions()


chrome_options = Options()


#options.add_argument('--disable-infobars')
#options.add_argument('--disable-extentions')
#options.add_argument('start-maximized')

#options.add_argument('--window-size=2560,1440')
options.add_argument('--disable-gpu')

#chrome_options.add_argument('--ignore-certificate-errors')
chrome_options.add_argument('--allow-running-insecure-content')
chrome_options.add_argument("--user-data-dir=C:\\Users\\ekans\\AppData\\Local\\Google\\Chrome\\User Data")
chrome_options.add_argument("--profile-directory=Person 1")
#chrome_options.add_argument('--profile-directory=C:\\Users\\ekans\\AppData\\Local\\Google\\Chrome\\User Data\\Default')
#user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'
#chrome_options.add_argument(f'user-agent={user_agent}')
#chrome_options.add_argument('user-agent=Chrome/99.0.4844.51')
options.add_argument('--headless')
#options.add_argument('--remote-debugging-port=9222')





def send_mail(status,message):
    try:
        #driver = webdriver.Chrome(ChromeDriverManager().install())
        #driver = uc.Chrome(version_main=98,options=options,chrome_options=opts)
        driver = uc.Chrome(browser_executable_path='C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',options=options,chrome_options=chrome_options)
        
        driver.get('https://google.com')
        driver.get_screenshot_as_file("screenshot0.png")
        driver.get(r'https://accounts.google.com/signin/v2/identifier?continue='+\
        'https%3A%2F%2Fmail.google.com%2Fmail%2F&service=mail&sacu=1&rip=1'+\
        '&flowName=GlifWebSignIn&flowEntry = ServiceLogin')
        driver.implicitly_wait(15)
        driver.get_screenshot_as_file("screenshot1.png")
        loginBox = driver.find_element_by_xpath('//*[@id ="identifierId"]')
        loginBox.send_keys('ekansh.n111@gmail.com')
        print('a-')
        time.sleep(2)
        nextButton = driver.find_elements_by_xpath('//*[@id ="identifierNext"]')
        nextButton[0].click()


        time.sleep(2)
        print('b-')
        #passWordBox = WebDriverWait(driver, 5).until(
        #EC.element_to_be_clickable((By.XPATH, "//input[@name='password']")))
        #passWordBox = driver.find_element_by_xpath("//input[@name='password']")
        time.sleep(2)
        #passWordBox = driver.find_element_by_xpath('//*[@id="password"]/div[1]/div/div[1]/input')#.send_keys('1Ekanshngowda')
        #passWordBox= driver.findElement(by=By.XPATH,value=("//input[@type='password']"))
        driver.get_screenshot_as_file("screenshot2.png")
        passWordBox = driver.find_element_by_name('password')
        driver.get_screenshot_as_file("screenshot99.png")
        print(passWordBox)
        #passWordBox1 =  EC.element_to_be_clickable((By.ID,'password'))
        #print(passWordBox1)
        #passWordBox = driver.find_element_by_id('Passwd')
        print('b111-')
        #time.sleep(5)
        passWordBox.send_keys('1Ekanshngowda')
        time.sleep(2)

        print('c-')
        nextButton = driver.find_elements_by_xpath('//*[@id ="passwordNext"]')
        nextButton[0].click()
    
        print('Login Successful...!!')
        time.sleep(2)

        driver.find_element_by_xpath('//div[contains(text(),"Compose")]').click()

        time.sleep(2)

        toElem = driver.find_element_by_name("to")
        toElem.send_keys('ekansh.n111@gmail.com')

        time.sleep(2)
        today = date.today()
        subjElem = driver.find_element_by_name("subjectbox")
        subjElem.send_keys(f'Login Initiation {status} for {today}')

        time.sleep(2)

        bodyElem = driver.find_element_by_css_selector("div[aria-label='Message Body']")
        bodyElem.send_keys(message)

        time.sleep(2)

        sendElem = driver.find_element_by_xpath("//div[text()='Send']")
        sendElem.click()
        time.sleep(2)
    except:
        print('Login Failed')


if __name__ == '__main__':
    message = 'hi'
    send_mail('ab',message)'''

'''import os

dir_path = os.getcwd()

profile = os.path.join(dir_path, "profile", "facebook")

option.add_argument(r"user-data-dir={}".format(profile))

browser = webdriver.Chrome(options=option,executable_path='./chromedriver')'''


from math import floor


print(round((4.021*2),1)/2)