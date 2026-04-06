#from multiprocessing import Value
from kiteconnect import KiteConnect
from kiteconnect import KiteTicker
import distutils_compat  # noqa: F401
import undetected_chromedriver as uc
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
import time, pyotp
from chrome_version import detect_chrome_major_version
from Directories import *


options = uc.ChromeOptions()
options.headless = True
CHROME_VERSION = detect_chrome_major_version()

#Headless argument is given so that the web brower runs in background and is not triggered in forefront
#options.add_argument('--headless')
#options.add_argument("--kiosk")

#Runs on python 3.8 in vs code

#Fetch input values from the file
with open(KiteEkanshLogin,'r') as a:
        content = a.readlines()
        a.close()

user_id= content[0].strip('\n')
user_pwd = content[1].strip('\n')
api_key = content[2].strip('\n')
api_secret = content[3].strip('\n')
totp_key= content[4].strip('\n')

token = ''
def login_in_zerodha(api_key, api_secret, user_id, user_pwd, totp_key):

    
    try:
        #Sometimes the firewall can block the Browser from accessing the Kite API on google chrome, so if stuck in endless loading loophten check for the firewall status
        #driver is used to navigate the chrome,make sure that it matches with the version of chrome)
        #When facing error of HTTP Error 404: Not Found, update the chrome driver with command pip install undetected_chromedriver --upgrade, 
        #404 error can occur if there is no endpoint configured for the latest version of chrome in the driver

        #If the chrome browser is getting stuck in endless loading screen then it can be due
        #Python version not at 3.8,chrome version higher than 119..124,port number not accessible,
        #or the VS code terminal default profile not set to command prompt
        #modified script patcher.py and updated download url with a new url link, line 285
        driver_kwargs = {"options": options}
        if CHROME_VERSION is not None:
            print("Using Chrome major version -->" + str(CHROME_VERSION))
            driver_kwargs["version_main"] = CHROME_VERSION
        else:
            print("Chrome version detection failed; using undetected_chromedriver auto-detection")

        try:
            driver = uc.Chrome(**driver_kwargs)#,port=49187)
        except Exception:
            if CHROME_VERSION is None:
                raise

            print("Version-pinned launch failed; retrying with undetected_chromedriver auto-detection...")
            driver = uc.Chrome(options=options)
        webpagelink = f'https://kite.trade/connect/login?api_key={api_key}&v=3'
        driver.get(webpagelink)

        #Fetch login details
        login_id = WebDriverWait(driver, 10).until(lambda x: x.find_element(by = By.XPATH,value='//*[@id="userid"]'))
        login_id.send_keys(user_id)
        pwd = WebDriverWait(driver, 10).until(lambda x: x.find_element(by = By.XPATH,value='//*[@id="password"]'))
        pwd.send_keys(user_pwd)
        submit = WebDriverWait(driver, 10).until(lambda x: x.find_element(by = By.XPATH,value='//*[@id="container"]/div/div/div[2]/form/div[4]/button'))
        submit.click()
        time.sleep(2)
        
        #Field to be updated was changed in front end on 5-9-2023
        totp = WebDriverWait(driver, 10).until(lambda x: x.find_element(by=By.XPATH, value='//label[contains(text(),"TOTP")]/following-sibling::input[1]'))
        authkey = pyotp.TOTP(totp_key)
        totp.send_keys(authkey.now())

        # Click submit button if present (Zerodha may require explicit submit)
        try:
            submit_btn = WebDriverWait(driver, 5).until(
                lambda x: x.find_element(by=By.XPATH, value='//button[@type="submit"]')
            )
            submit_btn.click()
        except Exception:
            pass  # auto-submit may have already triggered

        # Wait for redirect with request_token in URL
        WebDriverWait(driver, 20).until(
            lambda x: 'request_token=' in x.current_url
        )

        #To split the Request Token from the returned link in which it is embedded
        url = driver.current_url
        initial_token = url.split('request_token=')[1]
        #print(initial_token)
        request_token = initial_token.split('&')[0]
        print("Request token"+"-->"+str(request_token))
        

        driver.close()

        #Generate the access token from the request token  
        kite = KiteConnect(api_key = api_key)
        data = kite.generate_session(request_token,api_secret)
        print("Access token"+"-->"+str(data['access_token']))
        token = data["access_token"] 

        #Populate the access token inside a file
        with open(KiteEkanshLoginAccessToken,'w') as f:
            f.write(token)
            f.close()

        '''if(token):
            send_mail('Successful',f'Initiating Login procedure for account {user_id} to the broker terminal with access code {token}')'''

        #exit(0)
        return kite

    except Exception as e:
        driver.quit()
        '''send_mail('Failed',f'Login attempt to the Broker Terminal for account {user_id} has failed with access token{token}')'''
        print('except')
        print(e)
        #driver.close()
        exit(1)

if __name__ == '__main__':
    login_in_zerodha(api_key, api_secret, user_id, user_pwd, totp_key)
