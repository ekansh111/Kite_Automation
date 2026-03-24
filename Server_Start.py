
import string
from flask import Flask, request, abort, jsonify
from flask_ngrok import run_with_ngrok
from json import loads
from Server_Order_Place import order
from Login_Auto3_Angel import Login_Angel_Api
from PlaceFNOTradesKite import LoopHashOrderRequest
from PlaceMonthlyContrctFNOtrades import *
from Server_Order_Handler import *
from Kite_Server_Order_Handler import *
from forecast_orchestrator import ForecastOrchestrator
import os

app = Flask(__name__)
run_with_ngrok(app,subdomain="test111")#test111 subdomain for testing
json = ""
raw_data = ""
print(app)

# Initialize orchestrator (config only, no threads yet).
# .start() is called in __main__ guard to avoid duplicate workers
# when Flask reloader imports this module twice.
orchestrator = ForecastOrchestrator()
@app.route('/', methods=['POST'])


#Function to listen to a webhook, Trading View sends data here in the format specified in the else part,if not a json then the function returns none
#If any value is sent it tried to be parsed in the format specified and forwarded to the order function where the kite API is called and order placed
def webhook():
    if request.method == 'POST':

        order_details_fetch = request.get_json()
        if(order_details_fetch == None):            
            return 'Server is Up,No values sent',200
        else:
            if order_details_fetch.get("UpdatedOrderRouting") == 'True':
                if order_details_fetch.get("Broker") == 'ZERODHA':
                    ControlOrderFlowKite(order_details_fetch)
                    return 'success',200
                else:
                    ControlOrderFlowAngel(order_details_fetch)          
                    return 'success',200

            elif order_details_fetch.get("Broker") == 'ANGEL':
                Broker = order_details_fetch['Broker']

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


# ─── Forecast Orchestrator Routes ─────────────────────────────────

@app.route('/forecast', methods=['POST'])
def forecast_webhook():
    """
    Receives new 4-field webhook from TradingView for the forecast orchestrator.
    Expected JSON: {"SystemName":"S30A_GoldM","Instrument":"GOLDM","Netposition":1,"ATR":1200}
    Returns 200 immediately; processing happens in background worker thread.
    """
    payload = request.get_json()
    if payload is None:
        return 'No JSON payload', 400

    result = orchestrator.HandleWebhook(payload)
    status_code = 400 if result.get("status") == "error" else 200
    return jsonify(result), status_code


@app.route('/override', methods=['POST'])
def override():
    """
    Manual override endpoint.
    JSON: {"instrument":"GOLDM","override_type":"FORCE_FLAT"} or
          {"instrument":"GOLDM","override_type":"SET_POSITION","value":5} or
          {"instrument":"GOLDM","override_type":"CLEAR"}
    """
    payload = request.get_json()
    if payload is None:
        return 'No JSON payload', 400

    instrument = payload.get("instrument")
    override_type = payload.get("override_type")
    value = payload.get("value")

    if not instrument or not override_type:
        return jsonify({"status": "error", "message": "instrument and override_type required"}), 400

    result = orchestrator.ApplyOverride(instrument, override_type, value)
    return jsonify(result), 200


@app.route('/status', methods=['GET'])
def status():
    """Returns orchestrator status: forecasts, positions, overrides, recent orders."""
    return jsonify(orchestrator.GetStatus()), 200


if __name__ == '__main__':
    # Start orchestrator worker threads only in the main process.
    # With Flask reloader (use_reloader=True), the module gets imported twice:
    # once in the parent watcher process and once in the child.
    # WERKZEUG_RUN_MAIN is set only in the child (actual server) process.
    # Without reloader, this env var is absent so we always start.
    if os.environ.get('WERKZEUG_RUN_MAIN') == 'true' or not app.debug:
        orchestrator.Start()

    #app.run(host='0.0.0.0', port=80)
    app.run(port=5055)
    #2 ISSUE TIME IS CHAMGED TO 10 FOR STARTING AND AFTER CANCELLED ALSO ORDER PLACE

