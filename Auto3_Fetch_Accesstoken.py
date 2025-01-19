"""
This script demonstrates how to log into Zerodha (Kite) using TOTP-based 2FA
and store the resulting access token in a local file. The code uses OOP concepts
for clarity and maintainability. 

We also show how to read user credentials from a file that contains:
    Line 0: user_id
    Line 1: user_pwd
    Line 2: api_key
    Line 3: api_secret
    Line 4: totp_key

You can adapt this pattern to handle multiple user files or expand to multiple
accounts with minimal changes.
"""

import time
import pyotp
import undetected_chromedriver as uc
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from kiteconnect import KiteConnect
from Directories import KiteEkanshLogin,KiteRashmiLogin, KiteEkanshLoginAccessToken, KiteRashmiLoginAccessToken  # Example paths in "Directories.py"


class ZerodhaLogin:
    """
    A class to automate the login flow to Zerodha (Kite) using 
    undetected_chromedriver, TOTP-based 2FA, and Python.
    """

    def __init__(self, user_id, user_pwd, api_key, api_secret, totp_key,
                 output_file, chrome_version=131):
        """
        :param user_id: Zerodha login user ID
        :param user_pwd: Zerodha login password
        :param api_key: Kite API key (from Zerodha developer portal)
        :param api_secret: Kite API secret (from Zerodha developer portal)
        :param totp_key: TOTP secret key for 2FA
        :param output_file: Path to write the generated access token
        :param chrome_version: The major version of Chrome installed on your system
        """
        self.user_id = user_id
        self.user_pwd = user_pwd
        self.api_key = api_key
        self.api_secret = api_secret
        self.totp_key = totp_key
        self.output_file = output_file
        self.chrome_version = chrome_version

        # Prepare undetected Chrome options (headless by default)
        self.options = uc.ChromeOptions()
        self.options.headless = False#True  # set to False if you want to see the browser

        self.driver = None

    def _launchBrowser(self):
        """
        Internal method to create and return a Chrome driver instance 
        using undetected_chromedriver.
        """
        try:
            driver = uc.Chrome(version_main=self.chrome_version, options=self.options)
            return driver
        except Exception as e:
            print("Error initializing undetected_chromedriver:", e)
            raise e

    def _fetchRequestToken(self):
        """
        Internal method that:
        1. Navigates to the Kite login URL
        2. Inputs user credentials (user_id, user_pwd)
        3. Inputs TOTP
        4. Returns the request_token from the redirected URL
        """
        # 1. Launch the browser
        self.driver = self._launchBrowser()

        # 2. Open the Kite login page
        login_url = f'https://kite.trade/connect/login?api_key={self.api_key}&v=3'
        self.driver.get(login_url)

        # 3. Enter user ID and password
        login_id = WebDriverWait(self.driver, 10).until(
            lambda x: x.find_element(by=By.XPATH, value='//*[@id="userid"]')
        )
        login_id.send_keys(self.user_id)

        pwd = WebDriverWait(self.driver, 10).until(
            lambda x: x.find_element(by=By.XPATH, value='//*[@id="password"]')
        )
        pwd.send_keys(self.user_pwd)

        submit = WebDriverWait(self.driver, 10).until(
            lambda x: x.find_element(by=By.XPATH, value='//*[@id="container"]/div/div/div[2]/form/div[4]/button')
        )
        submit.click()

        time.sleep(2)

        # 4. Enter TOTP
        totp_element = WebDriverWait(self.driver, 10).until(
            lambda x: x.find_element(by=By.ID, value='userid')
        )
        authkey = pyotp.TOTP(self.totp_key)
        totp_element.send_keys(authkey.now())

        # Give some buffer time for login processing and redirect
        time.sleep(10)

        # 5. Extract the request_token from the current URL
        current_url = self.driver.current_url
        if 'request_token=' not in current_url:
            raise Exception(f"Could not find 'request_token' in URL. Current URL: {current_url}")

        initial_token = current_url.split('request_token=')[1]
        request_token = initial_token.split('&')[0]
        print("Request token -->", request_token)

        return request_token

    def _closeBrowser(self):
        """
        Close the browser instance if it is running.
        """
        if self.driver:
            self.driver.quit()
            self.driver = None

    def loginAndGenerateAccessToken(self):
        """
        Main public method to:
        1. Retrieve the request_token from the login flow
        2. Generate a new access token via the KiteConnect session
        3. Write the new access token to the specified file (output_file)
        4. Return a KiteConnect instance (with the new token set)
        """
        request_token = None
        try:
            request_token = self._fetchRequestToken()
        except Exception as e:
            print("Exception during login flow:", e)
            self._closeBrowser()
            return None

        # Close the browser after fetching the request token
        self._closeBrowser()

        # 6. Generate the access token using the request token and api_secret
        print("Generating session using request_token and api_secret...")
        kite = KiteConnect(api_key=self.api_key)
        try:
            data = kite.generate_session(request_token, self.api_secret)
            access_token = data['access_token']
            print("Access token -->", access_token)
        except Exception as e:
            print("Error generating session:", e)
            return None

        # 7. Write the new access token to a file
        try:
            with open(self.output_file, 'w') as f:
                f.write(access_token)
            print(f"Access token written to {self.output_file}")
        except Exception as e:
            print(f"Failed to write access token to {self.output_file}:", e)
            return None

        # Set this token on the KiteConnect instance
        kite.set_access_token(access_token)
        return kite

def runZerodhaLogin(login_file,OPAccessTokenFile):
    """
    Illustrates reading the input values from a file (e.g. KiteEkanshLogin)
    and using them to perform a login, then store the access token.
    
    :param login_file: Path to the file containing the user's login credentials.
    """
    # Fetch input values from the file
    with open(login_file, 'r') as cred_file:
        content = cred_file.readlines()
        cred_file.close()

    # Each line is read in order:
    user_id   = content[0].strip('\n')
    user_pwd  = content[1].strip('\n')
    api_key   = content[2].strip('\n')
    api_secret= content[3].strip('\n')
    totp_key  = content[4].strip('\n')

    # Create an instance of ZerodhaLogin with these credentials
    zlogin = ZerodhaLogin(
        user_id    = user_id,
        user_pwd   = user_pwd,
        api_key    = api_key,
        api_secret = api_secret,
        totp_key   = totp_key,
        output_file= OPAccessTokenFile,  # example output path
        chrome_version=131
    )

    kite_instance = zlogin.loginAndGenerateAccessToken()
    if kite_instance:
        print("Successfully generated a new session!")
    else:
        print("Failed to generate a new session.")


if __name__ == '__main__':
    # Pass KiteEkanshLogin as the file containing the user's credentials
    runZerodhaLogin(KiteEkanshLogin,KiteEkanshLoginAccessToken)
    runZerodhaLogin(KiteRashmiLogin,KiteRashmiLoginAccessToken)
