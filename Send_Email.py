import time
from click import option
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import undetected_chromedriver as uc
from webdriver_manager.chrome import ChromeDriverManager
from datetime import date
from webdriver_manager import WebDriverWait

options = uc.ChromeOptions()

options.add_argument('--headless')
options.add_argument('--window-size=1920,1080')
options.add_argument('--disable-gpu')



def send_mail(status,message):
    try:
        #driver = webdriver.Chrome(ChromeDriverManager().install())
        driver = uc.Chrome(version_main=98,options=options)
        driver.get(r'https://accounts.google.com/signin/v2/identifier?continue='+\
        'https%3A%2F%2Fmail.google.com%2Fmail%2F&service=mail&sacu=1&rip=1'+\
        '&flowName=GlifWebSignIn&flowEntry = ServiceLogin')
        driver.implicitly_wait(15)
    
        loginBox = driver.find_element_by_xpath('//*[@id ="identifierId"]')
        loginBox.send_keys('ekansh.n111@gmail.com')
        print('a')
        nextButton = driver.find_elements_by_xpath('//*[@id ="identifierNext"]')
        nextButton[0].click()
        print('b')
        passWordBox = driver.find_element_by_xpath("//input[@name='password']")
        print('b1')
        passWordBox.send_keys('1Ekanshngowda')
        print('c')
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
    send_mail('ab',message)
