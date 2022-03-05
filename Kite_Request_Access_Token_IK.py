import logging
from kiteconnect import KiteConnect
from datetime import datetime
from datetime import date

logging.basicConfig(level=logging.DEBUG)
with open('C:/Users/ekans/Documents/inputs/api_key_IK.txt','r') as a:
        api_key = a.read()
        a.close()
kite = KiteConnect(api_key=api_key)

# Redirect the user to the login url obtained
# from kite.login_url(), and receive the request_token
# from the registered redirect url after the login flow.
# Once you have the request_token, obtain the access_token
# as follows.

with open('C:/Users/ekans/Documents/inputs/api_secret_IK.txt','r') as a:
        api_secret = a.read()
        a.close()
print("login here:",kite.login_url())
req_tkn = input("Enter the request token")
data = kite.generate_session(req_tkn, api_secret=api_secret)
kite.set_access_token(data["access_token"])
token = data["access_token"] 
print(data["access_token"])

with open('C:/Users/ekans/Documents/inputs/access_token_IK.txt','w') as f:
        f.write(token)
        f.close()
