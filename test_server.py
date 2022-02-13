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
<<<<<<< HEAD
        #print(request.json())
        #json = request.get_json()
        #parse_json = request._parse_content_type(json)
        #print("parse_json" + parse_json)
        #print(request.is_json)
        #print(request.get_data())
        #raw_data = request.get_data(as_text=True)
        #print(bool(raw_data))
        #print(raw_data)
        #test
        #print(request.get_json())
        order_details_fetch = request.get_json()
        print(order_details_fetch)
        order()
=======

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
        #Price = order_details_fetch['Price'] or ''
        #print(Tradetype+Exchange+Tradingsymbol+Quantity+Variety+Ordertype+Product+Validity)


        order(Tradetype,Exchange,Tradingsymbol,Quantity,Variety,Ordertype,Product,Validity)
>>>>>>> master_2
        
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
<<<<<<< HEAD
    order()
=======
>>>>>>> master_2
