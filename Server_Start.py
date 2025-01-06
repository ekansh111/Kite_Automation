
import string
from flask import Flask, request, abort
from flask_ngrok import run_with_ngrok
from json import loads
from Server_Order_Place import order
from Login_Auto3_Angel import Login_Angel_Api
from PlaceFNOTradesKite import LoopHashOrderRequest
from PlaceMonthlyContrctFNOtrades import *
from Server_Order_Handler import *

app = Flask(__name__)
run_with_ngrok(app,subdomain="test111")#test111 subdomain for testing
json = ""
raw_data = ""
print(app)
@app.route('/', methods=['POST'])


#Function to listen to a webhook, Trading View sends data here in the format specified in the else part,if not a json then the function returns none
#If any value is sent it tried to be parsed in the format specified and forwarded to the order function where the kite API is called and order placed
def webhook():
    if request.method == 'POST':

        order_details_fetch = request.get_json()
        print(order_details_fetch)
        #print(order_details_fetch)
        if(order_details_fetch == None):            
            return 'Server is Up,No values sent',200
        else:
            if order_details_fetch.get("UpdatedOrderRouting") == 'True':
                ControlOrderFlowAngel(order_details_fetch)

                return 'success',200

            elif order_details_fetch.get("Broker") == 'ANGEL':
            #if order_details_fetch['Broker'] == 'ANGEL':
                Broker = order_details_fetch['Broker']
                #print(order_details_fetch)

            #If the request is to place an option order through API
            elif (order_details_fetch.get("Option") != None) and (order_details_fetch.get("Option").get("Broker") == 'ZERODHA_OPTION'):
                if order_details_fetch.get("Option").get("OptionType") == 'MonthlyOption':
                    print(order_details_fetch)
                    set_week_based_sl(order_details_fetch)
                    
                LoopHashOrderRequest(order_details_fetch)
                Broker = 'null'
                #Without the below return statement it causes the function to be called 4 times and the it causes order to be placed 4 times
                return 'success',200

            else:
                Broker = 'null'
            #print(Tradetype+Exchange+Tradingsymbol+Quantity+Variety+Ordertype+Product+Validity)

            if Broker == 'ANGEL':
                Login_Angel_Api(order_details_fetch)
            else:
                order(order_details_fetch)#Tradetype,Exchange,Tradingsymbol,Quantity,Variety,Ordertype,Product,Validity,Price)
            
            print("null")#DO NOT REMOVE,last line wasnt executed or some other error of same sort, thats why print statement is added
            return 'success',200
    else:
        abort(400)

if __name__ == '__main__':
    #app.run(host='0.0.0.0', port=80)
    app.run()
    #2 ISSUE TIME IS CHAMGED TO 10 FOR STARTING AND AFTER CANCELLED ALSO ORDER PLACE
    
