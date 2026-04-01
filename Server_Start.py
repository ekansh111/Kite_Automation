
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
import logging
import threading
import uuid

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
Logger = logging.getLogger(__name__)
NGROK_DOMAIN = os.environ.get("NGROK_DOMAIN", "listen.ngrok.io")
ANGEL_ORDER_WEBHOOK_PATH = "/forecast/angel"
ANGEL_EXECUTOR_PATH = os.environ.get("ANGEL_EXECUTOR_PATH", "/internal/angel-execute")
ANGEL_EXECUTOR_TOKEN = (os.environ.get("ANGEL_EXECUTOR_TOKEN") or "").strip()
DISABLE_NGROK = str(os.environ.get("DISABLE_NGROK", "false")).strip().lower() in {"true", "1", "yes", "y"}
ANGEL_EXECUTOR_ONLY = str(os.environ.get("ANGEL_EXECUTOR_ONLY", "false")).strip().lower() in {"true", "1", "yes", "y"}
_ANGEL_EXECUTION_REQUESTS = {}
_ANGEL_EXECUTION_REQUESTS_LOCK = threading.Lock()


def _RequestLogPayload(payload):
    if not isinstance(payload, dict):
        return payload

    keys = [
        "User", "Broker", "Exchange", "Tradingsymbol", "Symboltoken",
        "Tradetype", "Ordertype", "Variety", "Product", "Validity",
        "Quantity", "Price", "Netposition", "UpdatedOrderRouting",
        "ContractNameProvided", "InstrumentType",
    ]
    return {key: payload.get(key) for key in keys if key in payload}


def _IsTruthy(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _BuildWebhookResponsePayload(request_id, flow, *, result=None, order_details=None, status=None, message=None):
    payload = {
        "request_id": request_id,
        "flow": flow,
    }

    if order_details:
        payload["order"] = _RequestLogPayload(order_details)
        if order_details.get("ExecutionRoute") is not None:
            payload["execution_route"] = order_details.get("ExecutionRoute")
        if order_details.get("OrderId") is not None:
            payload["order_id"] = order_details.get("OrderId")
        if order_details.get("LastOrderWarning"):
            payload["warning"] = order_details.get("LastOrderWarning")

    result_status = None
    if result is not None:
        payload["result"] = result
        if isinstance(result, dict):
            result_status = str(result.get("status") or "").strip().lower() or None

    error_message = None
    if order_details and order_details.get("LastOrderError"):
        error_message = order_details.get("LastOrderError")
    elif isinstance(result, dict) and result.get("error"):
        error_message = result.get("error")

    if status is None:
        if error_message:
            status = "error"
        elif result_status in {"partial_failure", "failed", "rejected", "error"}:
            status = "error"
        else:
            status = "success"

    payload["status"] = status
    if message is not None:
        payload["message"] = message
    if error_message is not None:
        payload["error"] = error_message
    return payload


def _BuildProcessingResponsePayload(request_id):
    return {
        "request_id": request_id,
        "flow": "angel_internal",
        "status": "processing",
        "message": "Execution for this request_id is already in progress.",
    }


def _BuildWorkerOnlyResponse(request_id, flow):
    return {
        "request_id": request_id,
        "flow": flow,
        "status": "error",
        "message": "This server is running in ANGEL_EXECUTOR_ONLY mode. Use the internal executor endpoint only.",
    }


def _GetAuthorizationBearerToken():
    auth_header = request.headers.get("Authorization", "")
    prefix = "Bearer "
    if auth_header.startswith(prefix):
        return auth_header[len(prefix):].strip()
    return ""


def _RequireInternalExecutorAuth():
    if not ANGEL_EXECUTOR_TOKEN:
        return None

    provided_token = _GetAuthorizationBearerToken()
    if provided_token != ANGEL_EXECUTOR_TOKEN:
        return jsonify(
            {
                "status": "error",
                "flow": "angel_internal",
                "message": "Unauthorized internal execution request.",
            }
        ), 401
    return None


def _RejectIfExecutorOnly(flow):
    if not ANGEL_EXECUTOR_ONLY:
        return None

    request_id = str(uuid.uuid4())[:8]
    Logger.warning(
        "Rejected request in ANGEL_EXECUTOR_ONLY mode | request_id=%s path=%s remote_addr=%s flow=%s",
        request_id,
        request.path,
        request.remote_addr,
        flow,
    )
    return jsonify(_BuildWorkerOnlyResponse(request_id, flow)), 404

app = Flask(__name__)
if not DISABLE_NGROK:
    run_with_ngrok(app, subdomain=NGROK_DOMAIN)
json = ""
raw_data = ""
print(app)

orchestrator = None


def _GetOrchestrator():
    global orchestrator
    if orchestrator is None:
        # Config is loaded once lazily so worker-only mode avoids forecast DB startup.
        orchestrator = ForecastOrchestrator()
    return orchestrator


@app.route('/', methods=['POST'])
@app.route(ANGEL_ORDER_WEBHOOK_PATH, methods=['POST'])


#Function to listen to a webhook, Trading View sends data here in the format specified in the else part,if not a json then the function returns none
#If any value is sent it tried to be parsed in the format specified and forwarded to the order function where the kite API is called and order placed
def webhook():
    worker_only_response = _RejectIfExecutorOnly("worker_only")
    if worker_only_response is not None:
        return worker_only_response

    if request.method == 'POST':
        request_id = str(uuid.uuid4())[:8]
        Logger.info(
            "Webhook request received | request_id=%s path=%s remote_addr=%s content_type=%s",
            request_id,
            request.path,
            request.remote_addr,
            request.content_type,
        )
        order_details_fetch = None
        try:
            order_details_fetch = request.get_json()
            if(order_details_fetch == None):            
                Logger.info("Webhook request had no JSON payload | request_id=%s", request_id)
                return jsonify(
                    _BuildWebhookResponsePayload(
                        request_id,
                        "webhook",
                        status="noop",
                        message="Server is up. No JSON payload was sent.",
                    )
                ), 200
            else:
                Logger.info(
                    "Webhook payload parsed | request_id=%s payload=%s",
                    request_id,
                    _RequestLogPayload(order_details_fetch),
                )
                UseUpdatedRouting = _IsTruthy(order_details_fetch.get("UpdatedOrderRouting"))
                ForceAngelNcdexRoute = (
                    str(order_details_fetch.get("Broker", "")).strip().upper() == "ANGEL" and
                    str(order_details_fetch.get("Exchange", "")).strip().upper() == "NCDEX"
                )

                if UseUpdatedRouting or ForceAngelNcdexRoute:
                    if order_details_fetch.get("Broker") == 'ZERODHA':
                        Logger.info("Dispatching request to Zerodha flow | request_id=%s", request_id)
                        Result = ControlOrderFlowKite(order_details_fetch)
                        Logger.info("Zerodha flow returned control | request_id=%s", request_id)
                        return jsonify(
                            _BuildWebhookResponsePayload(
                                request_id,
                                "zerodha",
                                result=Result,
                                order_details=order_details_fetch,
                            )
                        ), 200
                    elif order_details_fetch.get("Broker") == 'ANGEL':
                        Logger.info(
                            "Dispatching request to Angel flow | request_id=%s reason=%s",
                            request_id,
                            "forced_ncdex_route" if ForceAngelNcdexRoute and not UseUpdatedRouting else "updated_routing",
                        )
                        Result = ControlOrderFlowAngel(order_details_fetch)
                        Logger.info(
                            "Angel flow returned | request_id=%s result=%s last_error=%s",
                            request_id,
                            Result,
                            order_details_fetch.get("LastOrderError"),
                        )
                        return jsonify(
                            _BuildWebhookResponsePayload(
                                request_id,
                                "angel",
                                result=Result,
                                order_details=order_details_fetch,
                            )
                        ), 200
                    else:
                        Logger.info(
                            "Updated routing requested for unsupported broker | request_id=%s broker=%s",
                            request_id,
                            order_details_fetch.get("Broker"),
                        )
                        return jsonify(
                            _BuildWebhookResponsePayload(
                                request_id,
                                "webhook",
                                order_details=order_details_fetch,
                                status="error",
                                message=f"Updated routing is not supported for broker {order_details_fetch.get('Broker')!r}.",
                            )
                        ), 400

                elif order_details_fetch.get("Broker") == 'ANGEL':
                    Broker = order_details_fetch['Broker']
                    Logger.info("Dispatching legacy Angel login flow | request_id=%s", request_id)

                #If the request is to place an option order through API
                elif (order_details_fetch.get("Option") != None) and (order_details_fetch.get("Option").get("Broker") == 'ZERODHA_OPTION'):
                    if order_details_fetch.get("Option").get("OptionType") == 'MonthlyOption':
                        print(order_details_fetch)
                        set_week_based_sl(order_details_fetch)
                        
                    LoopHashOrderRequest(order_details_fetch)
                    Broker = 'null'
                    Logger.info("Completed Zerodha option flow | request_id=%s", request_id)
                    #Without the below return statement it causes the function to be called 4 times and the it causes order to be placed 4 times
                    return jsonify(
                        _BuildWebhookResponsePayload(
                            request_id,
                            "zerodha_option",
                            order_details=order_details_fetch,
                            message="Zerodha option flow completed.",
                        )
                    ), 200

                else:
                    Broker = 'null'
                #print(Tradetype+Exchange+Tradingsymbol+Quantity+Variety+Ordertype+Product+Validity)

                if Broker == 'ANGEL':
                    Login_Angel_Api(order_details_fetch)
                else:
                    order(order_details_fetch)#Tradetype,Exchange,Tradingsymbol,Quantity,Variety,Ordertype,Product,Validity,Price)
                
                print("null")#DO NOT REMOVE,last line wasnt executed or some other error of same sort, thats why print statement is added
                Logger.info("Webhook flow completed | request_id=%s broker=%s", request_id, Broker)
                return jsonify(
                    _BuildWebhookResponsePayload(
                        request_id,
                        "legacy_angel" if Broker == 'ANGEL' else "legacy_default",
                        order_details=order_details_fetch,
                        message="Legacy webhook flow completed.",
                    )
                ), 200
        except Exception:
            Logger.exception(
                "Webhook request failed | request_id=%s payload=%s",
                request_id,
                _RequestLogPayload(order_details_fetch),
            )
            raise
    else:
        abort(400)


# ─── Forecast Orchestrator Routes ─────────────────────────────────

@app.route('/forecast', methods=['POST'])
def forecast_webhook():
    """
    Receives new 4-field webhook from TradingView for the forecast orchestrator.
    Expected JSON: {"SystemName":"S30A_GoldM","Instrument":"GOLDM","Netposition":1,"ATR":1200,"LTP":72500.0}
    Returns 200 immediately; processing happens in background worker thread.
    """
    worker_only_response = _RejectIfExecutorOnly("forecast")
    if worker_only_response is not None:
        return worker_only_response

    payload = request.get_json()
    if payload is None:
        return 'No JSON payload', 400

    result = _GetOrchestrator().HandleWebhook(payload)
    status_code = 400 if result.get("status") == "error" else 200
    return jsonify(result), status_code


@app.route(ANGEL_EXECUTOR_PATH, methods=['POST'])
def internal_angel_execute():
    auth_failure = _RequireInternalExecutorAuth()
    if auth_failure is not None:
        return auth_failure

    payload = request.get_json()
    if not isinstance(payload, dict):
        return jsonify(
            {
                "status": "error",
                "flow": "angel_internal",
                "message": "JSON payload required.",
            }
        ), 400

    request_id = str(payload.get("request_id") or "").strip() or str(uuid.uuid4())[:8]
    order_details = payload.get("order")
    if not isinstance(order_details, dict):
        return jsonify(
            {
                "request_id": request_id,
                "status": "error",
                "flow": "angel_internal",
                "message": "Payload must include an 'order' object.",
            }
        ), 400

    Logger.info(
        "Internal Angel execution request received | request_id=%s remote_addr=%s source=%s order=%s",
        request_id,
        request.remote_addr,
        payload.get("source"),
        _RequestLogPayload(order_details),
    )

    with _ANGEL_EXECUTION_REQUESTS_LOCK:
        cached_response = _ANGEL_EXECUTION_REQUESTS.get(request_id)
        if cached_response is not None:
            if cached_response.get("status") == "processing":
                return jsonify(cached_response), 202
            Logger.info("Returning cached Angel internal execution response | request_id=%s", request_id)
            return jsonify(cached_response), 200

        _ANGEL_EXECUTION_REQUESTS[request_id] = _BuildProcessingResponsePayload(request_id)

    try:
        result = ControlOrderFlowAngel(order_details)
        response_payload = _BuildWebhookResponsePayload(
            request_id,
            "angel_internal",
            result=result,
            order_details=order_details,
        )
        status_code = 200
    except Exception as exc:
        Logger.exception(
            "Internal Angel execution failed | request_id=%s order=%s",
            request_id,
            _RequestLogPayload(order_details),
        )
        order_details.setdefault("LastOrderError", str(exc))
        response_payload = _BuildWebhookResponsePayload(
            request_id,
            "angel_internal",
            result=None,
            order_details=order_details,
            status="error",
        )
        status_code = 500

    with _ANGEL_EXECUTION_REQUESTS_LOCK:
        _ANGEL_EXECUTION_REQUESTS[request_id] = response_payload

    return jsonify(response_payload), status_code


@app.route('/override', methods=['POST'])
def override():
    """
    Manual override endpoint.
    JSON: {"instrument":"GOLDM","override_type":"FORCE_FLAT"} or
          {"instrument":"GOLDM","override_type":"SET_POSITION","value":5} or
          {"instrument":"GOLDM","override_type":"CLEAR"}
    """
    worker_only_response = _RejectIfExecutorOnly("override")
    if worker_only_response is not None:
        return worker_only_response

    payload = request.get_json()
    if payload is None:
        return 'No JSON payload', 400

    instrument = payload.get("instrument")
    override_type = payload.get("override_type")
    value = payload.get("value")

    if not instrument or not override_type:
        return jsonify({"status": "error", "message": "instrument and override_type required"}), 400

    result = _GetOrchestrator().ApplyOverride(instrument, override_type, value)
    return jsonify(result), 200


@app.route('/status', methods=['GET'])
def status():
    """Returns orchestrator status: forecasts, positions, overrides, recent orders."""
    worker_only_response = _RejectIfExecutorOnly("status")
    if worker_only_response is not None:
        return worker_only_response

    return jsonify(_GetOrchestrator().GetStatus()), 200


if __name__ == '__main__':
    Logger.info(
        "Starting webhook server | ngrok_domain=%s ngrok_disabled=%s executor_only=%s order_paths=/,%s forecast_path=/forecast internal_angel_path=%s status_path=/status port=5055",
        NGROK_DOMAIN if not DISABLE_NGROK else None,
        DISABLE_NGROK,
        ANGEL_EXECUTOR_ONLY,
        ANGEL_ORDER_WEBHOOK_PATH,
        ANGEL_EXECUTOR_PATH,
    )
    # Start orchestrator worker threads only in the main process.
    # With Flask reloader (use_reloader=True), the module gets imported twice:
    # once in the parent watcher process and once in the child.
    # WERKZEUG_RUN_MAIN is set only in the child (actual server) process.
    # Without reloader, this env var is absent so we always start.
    if (os.environ.get('WERKZEUG_RUN_MAIN') == 'true' or not app.debug) and not ANGEL_EXECUTOR_ONLY:
        _GetOrchestrator().Start()

    #app.run(host='0.0.0.0', port=80)
    app.run(port=5055)
    #2 ISSUE TIME IS CHAMGED TO 10 FOR STARTING AND AFTER CANCELLED ALSO ORDER PLACE
