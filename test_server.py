from flask import Flask, request, abort
from flask_ngrok import run_with_ngrok
from json import loads
from Kite_test import order
app = Flask(__name__)
run_with_ngrok(app)
json = ""
raw_data = ""

@app.route('/', methods=['POST'])
def webhook():
    if request.method == 'POST':

        order_details_fetch = request.get_json()
        print(order_details_fetch)

        Tradetype = order_details_fetch['Tradetype']
        Exchange = order_details_fetch['Exchange']
        Tradingsymbol = order_details_fetch['Tradingsymbol']
        Quantity = order_details_fetch['Quantity']
        Variety = order_details_fetch['Variety']
        Ordertype = order_details_fetch['Ordertype']
        Product = order_details_fetch['Product']
        Validity = order_details_fetch['Validity']
        Price = order_details_fetch['Price'] or 0.0
        #Price = order_details_fetch['Price'] or ''
        #print(Tradetype+Exchange+Tradingsymbol+Quantity+Variety+Ordertype+Product+Validity)


        order(Tradetype,Exchange,Tradingsymbol,Quantity,Variety,Ordertype,Product,Validity,Price)
        
        print("null")
        return 'success',200
    else:
        abort(400)
def parse():
        parse_data = json.loads(raw_data)
        print(parse_data['not'])
if __name__ == '__main__':
    #app.run(host='0.0.0.0', port=80)
    app.run()
    parse()
