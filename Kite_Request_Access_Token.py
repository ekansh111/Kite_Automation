import logging
from kiteconnect import KiteConnect
from datetime import datetime
from datetime import date

logging.basicConfig(level=logging.DEBUG)

kite = KiteConnect(api_key="nget8iniou5mlnfj")

# Redirect the user to the login url obtained
# from kite.login_url(), and receive the request_token
# from the registered redirect url after the login flow.
# Once you have the request_token, obtain the access_token
# as follows.
print("login here:",kite.login_url())
req_tkn = input("Enter the request token")
data = kite.generate_session(req_tkn, api_secret="p9z3b74nfx5izarcr62w3xe111k8lnuz")
kite.set_access_token(data["access_token"])
token = data["access_token"] 
print(data["access_token"])

with open('C:/Users/ekans/Documents/Kite_API/inputs/access_token.txt','w') as f:
        f.write(token)
        f.close()
