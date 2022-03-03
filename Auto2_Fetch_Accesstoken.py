from kiteconnect import KiteConnect
from kiteconnect import KiteTicker
import undetected_chromedriver as uc
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
import time, pyotp

with open('C:/Users/ekans/Documents/inputs/Login_Credentials_IK.txt','r') as a:
        content = a.readlines()
        a.close()

user_id= content[0].strip('\n')
user_pwd = content[1].strip('\n')
api_key = content[2].strip('\n')
api_secret = content[3].strip('\n')
totp_key= content[4].strip('\n')

def login_in_zerodha(api_key, api_secret, user_id, user_pwd, totp_key):

    

    driver = uc.Chrome(version_main=98)
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
    #print(data['access_token'])
    token = data["access_token"] 
    with open('C:/Users/ekans/Documents/inputs/access_token_IK.txt','w') as f:
        f.write(token)
        f.close()

    return kite

if __name__ == '__main__':
    login_in_zerodha(api_key, api_secret, user_id, user_pwd, totp_key)