import logging
from SmartApi import SmartConnect
from Server_Order_Handler import EstablishConnectionAngelAPI

def fetchPositions(smartApi):
    """
    Fetches positions from Angel One using camelcase variables only.
    """
    logging.info("Fetching position book")
    positionsData = smartApi.position()

    logging.debug(f"Raw positions response: {positionsData}")

    if not positionsData.get("status"):
        logging.warning(f"Position fetch failed: {positionsData}")
        return []

    return positionsData.get("data", [])

def exitPosition(smartApi, singlePosition):
    """
    Exits the specified MIS position by placing a reverse MARKET order.
    """

    tradingSymbol = singlePosition["tradingsymbol"]
    exchangeName = singlePosition["exchange"]
    productType = singlePosition["producttype"]
    netQty = int(float(singlePosition["netqty"]))

    logging.info(f"Exiting MIS position for {tradingSymbol} with netQty={netQty}")

    if netQty == 0:
        logging.info(f"No net quantity for {tradingSymbol}, skipping exit")
        return {"message": "No net quantity to exit"}

    transactionType = "SELL" if netQty > 0 else "BUY"
    quantityVal = abs(netQty)

    orderParams = {
        "variety":         "NORMAL",
        "tradingsymbol":   tradingSymbol,
        "symboltoken":     singlePosition["symboltoken"],
        "transactiontype": transactionType,
        "exchange":        exchangeName,
        "ordertype":       "LIMIT",
        "producttype":     productType,
        "duration":        "DAY",
        "price":           singlePosition["ltp"],
        "quantity":        quantityVal,
    }

    try:
        response = smartApi.placeOrder(orderParams)
        logging.info(f"Exit order placed for {tradingSymbol}, response={response}")
        return response
    except Exception as e:
        logging.error(f"Failed to exit position for {tradingSymbol}: {e}")
        return {"error": str(e)}

def exitAllMisPositions(smartApi):
    """
    Fetches all positions, exits MIS positions using camelcase variables only.
    """
    allPositions = fetchPositions(smartApi)
    if not allPositions:
        print("No positions found")
        return

    for singlePos in allPositions:
        productType = singlePos["producttype"]
        if productType.upper() == "INTRADAY":
            exitPosition(smartApi, singlePos)

def main():
    logging.basicConfig(level=logging.INFO)
    
    OrderUserDetails = {"User": "E51339915"}
    smartApi = EstablishConnectionAngelAPI(OrderUserDetails)

    exitAllMisPositions(smartApi)

if __name__ == "__main__":
    main()
