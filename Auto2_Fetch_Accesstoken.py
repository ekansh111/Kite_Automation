from multiprocessing import Value
from kiteconnect import KiteConnect
from kiteconnect import KiteTicker
import undetected_chromedriver as uc
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
import time, pyotp


options = uc.ChromeOptions()

#Headless argument is given so that the web brower runs in background and is not triggered in forefront
options.add_argument('--headless')


#Fetch input values from the file
with open('C:/Users/ekans/OneDrive/Documents/inputs/Login_Credentials.txt','r') as a:
        content = a.readlines()
        a.close()

user_id= content[0].strip('\n')
user_pwd = content[1].strip('\n')
api_key = content[2].strip('\n')
api_secret = content[3].strip('\n')
totp_key= content[4].strip('\n')

token = ''
#print(api_key)
def login_in_zerodha(api_key, api_secret, user_id, user_pwd, totp_key):

    
    try:
        #Sometimes the firewall can block the Browser from accessing the Kite API on google chrome, so if stuck in endless loading loophten check for the firewall status
        #driver is used to navigate the chrome,make sure that it matches with the version of chrome)
        #When facing error of HTTP Error 404: Not Found, update the chrome driver with command pip install undetected_chromedriver --upgrade, 
        #404 error can occur if there is no endpoint configured for the latest version of chrome in the driver
        driver = uc.Chrome(version_main=118,options=options)
        driver.get(f'https://kite.trade/connect/login?api_key={api_key}&v=3')
        #Fetch login details
        login_id = WebDriverWait(driver, 10).until(lambda x: x.find_element(by = By.XPATH,value='//*[@id="userid"]'))
        login_id.send_keys(user_id)
        pwd = WebDriverWait(driver, 10).until(lambda x: x.find_element(by = By.XPATH,value='//*[@id="password"]'))
        pwd.send_keys(user_pwd)
        submit = WebDriverWait(driver, 10).until(lambda x: x.find_element(by = By.XPATH,value='//*[@id="container"]/div/div/div[2]/form/div[4]/button'))
        submit.click()
        time.sleep(2)
        #adjustment to code to include totp
        
        #Points to the field where the Totp key needs to be entered
#        totp = WebDriverWait(driver, 10).until(lambda x: x.find_element(by = By.XPATH,value='//label[text()="External TOTP"]/following-sibling::input'))
        #Field to be updated was changed in front end on 5-9-2023
        totp = WebDriverWait(driver, 10).until(lambda x: x.find_element(by = By.ID,value='userid'))
        #time.sleep(100)
        authkey = pyotp.TOTP(totp_key)
        totp.send_keys(authkey.now())
        #print(totp)
        #adjustment complete

        #Points to the continue button on the page
        #Continue button click is no longer needed as Zerodoha updated the website to autologin after the toptp is entered
        #continue_btn = WebDriverWait(driver, 10).until(lambda x: x.find_element(by = By.XPATH,value='//*[@id="container"]/div/div/div[2]/form/div[3]/button'))
        #continue_btn.click()
        
        print(driver.current_url)
        time.sleep(10)
        #print(driver.current_url)

        #To split the Request Token from the returned link in which it is embedded
        url = driver.current_url
        #print(url)
        initial_token = url.split('request_token=')[1]
        #print(initial_token)
        request_token = initial_token.split('&')[0]
        print(request_token)
        

        driver.close()

        #Generate the access token from the request token  
        kite = KiteConnect(api_key = api_key)
        data = kite.generate_session(request_token,api_secret)
        print(data['access_token'])
        token = data["access_token"] 

        #Populate the access token inside a file
        with open('C:/Users/ekans/OneDrive/Documents/inputs/access_token_IK.txt','w') as f:
            f.write(token)
            f.close()

        '''if(token):
            send_mail('Successful',f'Initiating Login procedure for account {user_id} to the broker terminal with access code {token}')'''

        return kite

    except Exception as e:
        '''send_mail('Failed',f'Login attempt to the Broker Terminal for account {user_id} has failed with access token{token}')'''
        print(e)

if __name__ == '__main__':
    login_in_zerodha(api_key, api_secret, user_id, user_pwd, totp_key)