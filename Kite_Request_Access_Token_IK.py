import logging
from kiteconnect import KiteConnect
from datetime import datetime
from datetime import date

logging.basicConfig(level=logging.DEBUG)

kite = KiteConnect(api_key="6222qaeth2qxmv2n")

# Redirect the user to the login url obtained
# from kite.login_url(), and receive the request_token
# from the registered redirect url after the login flow.
# Once you have the request_token, obtain the access_token
# as follows.
print("login here:",kite.login_url())
req_tkn = input("Enter the request token")
data = kite.generate_session(req_tkn, api_secret="4gm3v1rkp2522h0ketajrudm4jr3zvhd")
kite.set_access_token(data["access_token"])
token = data["access_token"] 
print(data["access_token"])

with open('C:/Users/ekans/Documents/Kite_API/inputs/access_token.txt','w') as f:
        f.write(token)
        f.close()
