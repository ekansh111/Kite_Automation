
import string
from flask import Flask, request, abort
from flask_ngrok import run_with_ngrok
from json import loads
from Server_Order_Place import order
app = Flask(__name__)
run_with_ngrok(app,subdomain="test111")#test11 subdomain for testing
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
        if(order_details_fetch == None):
            
            return 'Server is Up,No values sent',200
        else:
            Tradetype = order_details_fetch['Tradetype']
            Exchange = order_details_fetch['Exchange']
            Tradingsymbol = str(order_details_fetch['Tradingsymbol']).replace(" ","")
            Quantity = order_details_fetch['Quantity']
            Variety = order_details_fetch['Variety']
            Ordertype = order_details_fetch['Ordertype']
            Product = order_details_fetch['Product']
            Validity = order_details_fetch['Validity']
            Price = order_details_fetch['Price'] or 0.0
            #Price = order_details_fetch['Price'] or ''
            #print(Tradetype+Exchange+Tradingsymbol+Quantity+Variety+Ordertype+Product+Validity)


            order(Tradetype,Exchange,Tradingsymbol,Quantity,Variety,Ordertype,Product,Validity,Price)
            
            print("null")#DO NOT REMOVE,last line wasnt executed or some other error of same sort, thats why print statement is added
            return 'success',200
    else:
        abort(400)

if __name__ == '__main__':
    #app.run(host='0.0.0.0', port=80)
    app.run()
    
