import pandas as pd
from binance.client import Client
import datetime
import requests
import json
import ccxt
import dropbox
import csv
import boto3
import random
import os

pd.options.mode.chained_assignment = None  # default='warn'


'''
CCXT client for interacting with Binance. This can be used for various exchanges.
'''
api_key = os.environ["BinanceKey"]
api_secret = os.environ["BinanceSecret"]

bclient = Client(api_key=api_key, api_secret=api_secret)
ccxtClient = ccxt.binance({
    'apiKey': api_key,
    'secret': api_secret,
})

class BinanceExch():

    def __init__(self):
        self.params = {'test': False}

    def buy(self, market, amount, price):
        return (ccxtClient.create_order(
            symbol=market,
            type="limit",
            side="buy",
            amount=amount,
            price=price,
            params=self.params,
        ))

    def sell(self, market, amount, price):
        global params
        return (ccxtClient.create_order(
            symbol=market,
            type="limit",
            side="sell",
            amount=amount,
            price=price,
            params=self.params,
        ))

    def cancelOrders(self, market):
        orders = ccxtClient.fetch_open_orders(market)
        for order in orders:
            ccxtClient.cancel_order(order['info']['orderId'], market)

    def get_price(self, market):
        return float(ccxtClient.fetch_ticker(market)['info']['lastPrice'])

    def get_balance(self, coin):
        return float(ccxtClient.fetch_balance()[coin]['free'])

'''
AWS access using boto3. Lambda functions run "at least ones" per triggering event. boto3 is used to put a key into
dynamoDB while the script is running. Further function calls caused by the triggering event are ignored while this
key is present. When the script is finished running the key is removed from dynamoDB.
'''
aws_access_key_id = os.environ["AWSKey"]
aws_secret_access_key = os.environ["AWSSecret"]
region = "us-east-1"

dynamodb = boto3.resource('dynamodb', region_name=region, aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)

table = dynamodb.Table("KeyTable")

'''
Define a simple function for sending notification via pushbullet. Pushbullet is used to notify the user when script is
running, and of any successful or failed orders that occur.
'''
def pushbullet_message(title, body):
    msg = {"type": "note", "title": title, "body": body}
    TOKEN = os.environ["PushbulletToken"]
    resp = requests.post('https://api.pushbullet.com/v2/pushes',
                         data=json.dumps(msg),
                         headers={'Authorization': 'Bearer ' + TOKEN,
                                  'Content-Type': 'application/json'})
    if resp.status_code != 200:
        raise Exception('Error', resp.status_code)
    else:
        print('Message sent')

'''
A simple function that returns formatted historical price data from Binance for a given symbol. This is currently
set up to collect weekly bars.
'''
def binanceBarExtractor(symbol):
    today = datetime.datetime.now()
    start_date = datetime.datetime.now() - datetime.timedelta(days=250)
    klines = bclient.get_historical_klines(symbol, Client.KLINE_INTERVAL_1WEEK,
                                           start_date.strftime("%d %b %Y %H:%M:%S"),
                                           today.strftime("%d %b %Y %H:%M:%S"), 1000)
    data = pd.DataFrame(klines,
                        columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_av',
                                 'trades', 'tb_base_av', 'tb_quote_av', 'ignore'])
    data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')

    data.set_index('timestamp', inplace=True)
    return data


'''
Computes indicators for the close values for the given data frame.
Currently MACD is calculated using custom parameters.
'''
def getInds(df):
    fastP = 5
    slowP = 29
    sigP = 10

    df["EMAFast"] = df["close"].ewm(span=fastP).mean()
    df["EMASlow"] = df["close"].ewm(span=slowP).mean()
    df["MACD"] = df["EMAFast"] - df["EMASlow"]
    df["Sig"] = df["MACD"].ewm(span=sigP).mean()
    df["MACDHist"] = df["MACD"] - df["Sig"]

    return df


'''
Determines signals based on indicator conditions. Currently this is a simple trend following algorithm, buying when
the MACD is > 0 and selling when the MACD is < 0.
'''
def getSigs(df):
    sigList = []
    for index, row in df.iterrows():
        if row["MACDHist"] > 0:
            sigList.append(1)
        elif row["MACDHist"] < 0:
            sigList.append(-1)
        else:
            sigList.append(0)

    df["sigs"] = sigList
    return df

'''
AWS Lambda requires a lambdaHandler function call as a hook to start the function. 
This is basically the if __name__ == "__main__": for AWS Lambda.
'''
def lambdaHandler(event, context):
    global ordersMade

    '''
    Using try/except to test whether or not a run key is present in dynamoDB. If the key is present, the function is
    already running and we quit the function. If the key is not present, we write a key to the table and perform the
    remainder of the function.
    '''
    try:
        response = table.get_item(
            Key={
                "RequestID": "portfolioSignals"
            }
        )
        item = response['Item']

        print(item)

    except Exception as e:
        item = False

    if not item:
        table.put_item(
            Item={
                "RequestID": "portfolioSignals",
                "ttl": int(datetime.datetime.today().timestamp() + 300),
                "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "killtime": (datetime.datetime.today() + datetime.timedelta(seconds=300)).strftime("%Y-%m-%d %H:%M:%S")
            }
        )
    else:
        print("Already Running")
        return

    '''
    Grabs my ticker watchlist from dropbox. This is just a convenient way to have an editable watchlist without having
    to interact with AWS or edit code whenever I want to make changes. Currently this script only trades USDT pairs,
    and the tickerList file expects the following format:
    ADAUSDT,BTCUSDT,ETHUSDT
    
    There is no limit to the number of tickers than can be included in the list.
    '''

    dbx = dropbox.Dropbox(os.environ["DropboxToken"])
    binance = BinanceExch()
    path = '/Algo/AWS/tickersList.txt'

    metadata, f = dbx.files_download(path)
    csv_reader = csv.reader(f.content.decode().splitlines(), delimiter=',')


    '''
    The ticker list is shuffled to randomize the order in which they are analysed. This is to prevent bias toward
    specific tickers. This may seem odd, but backtesting this strategy on dozen of cryptocurrencies showed great
    performance using various strategies.
    '''
    tickers = next(csv_reader)
    random.shuffle(tickers)
    print(tickers)


    '''
    This logic determines how much of the total portfolio value will be used for each buy.
    The current logic will fail for 1 ticker. For 2 or more tickers it will buy approximately half of the tickers, with
    funds spread evenly among them.
    '''
    numTickers = len(tickers)
    portfolioWeight = 2 / numTickers


    '''
    This loop goes through each ticker in the list, and performs most of the trading logic.
    '''
    ordersMade = []
    failedOrders = 0
    dataList = []
    tradeSigList = []
    k = 0
    for i in tickers:

        '''
        We get the historical price data and compute the indicators and signals.
        '''
        bars = binanceBarExtractor(i)
        bars = bars.astype('float')
        bars = getInds(bars)
        bars = getSigs(bars)
        dataList.append([bars, i])

        '''
        This determines the current market position in USDT for the currently analysed ticker.
        '''
        market = dataList[k][1][:-4] + "/" + dataList[k][1][-4:]

        bal = binance.get_balance(market.split("/")[0])
        price = binance.get_price(market)

        realPos = bal * price


        '''
        If the strategy dictates a buy for the most recent bar and the currently held USDT value of the crypto
        is less than 30 then we execute a buy.
        '''
        if dataList[k][0]["MACDHist"][-1] > 0 and realPos < 30:

            '''
            This logic determines the total value of the portfolio in USDT
            '''
            USDTbalance = binance.get_balance(market.split("/")[1])

            rawBalance = ccxtClient.fetch_balance()['total']

            totbalance = pd.DataFrame.from_dict(rawBalance, orient='index')

            totbalance = totbalance.reset_index()
            totbalance.columns = ['symbol', 'amount']
            totbalance['symbol'] = totbalance['symbol'] + "/USDT"
            totbalance = totbalance[totbalance['amount'] != 0]

            USDTList = []

            for pair in totbalance['symbol']:
                if pair != "USDT/USDT":
                    lastVal = ccxtClient.fetch_ticker(pair)['info']['lastPrice']

                    USDTList.append(lastVal)
                else:
                    USDTList.append(1)

            totbalance['USDTPrice'] = USDTList
            totbalance['USDTPrice'] = totbalance['USDTPrice'].astype(float)

            totbalance['USDTVal'] = totbalance['amount'] * totbalance['USDTPrice']

            totbalance = round(totbalance['USDTVal'].sum(), 2)

            '''
            This logic determines how much of the target crypto should be purchased based on the portfolioWeight
            '''
            price = binance.get_price(market)

            if totbalance * portfolioWeight < USDTbalance * 0.6:
                balance = portfolioWeight * totbalance
            else:
                balance = 0.6 * USDTbalance
            amount = (balance / price) * 0.999  # 0.10% maker/taker fee without BNB


            '''
            The purchase is only performed if the actual USDT currently available is greater than 50. Orders a tracked
            using a list for later use.
            '''
            if balance > 50:
                tradeSigList.append(["Buy", market])
                try:
                    print("Buy ", market)
                    order = binance.buy(market, amount, price)

                    print()
                    print("Buy " + market)
                except:
                    print()
                    print("Order Failed")
                    print(market)
                    failedOrders = failedOrders + 1
            ordersMade.append([order['symbol'], order['side'], order['amount'], order['price'], order['status']])



        '''
        Similar to the buy logic, if the  most recent bar indicates a sell and the currently held value of the ticker
        is more than 10 USDT then we sell all of this ticker. Sell orders are tracked using the same list as the buys.
        '''
        if dataList[k][0]["MACDHist"][-1] < 0 and realPos > 10:
            amount = binance.get_balance(market.split("/")[0])
            price = binance.get_price(market)

            tradeSigList.append(["Sell", market])
            try:
                print("Sell ", market)
                order = binance.sell(market, amount, price)


            except:
                print()
                print("Order Failed")
                print(market)
                failedOrders = failedOrders + 1

            ordersMade.append([order['symbol'], order['side'], order['amount'], order['price'], order['status']])

        k = k + 1

    '''
    Pushbullet messages are used to help the user keep track of their algorithm activities.
    A message is sent whenever the system executes.
    '''
    pushbullet_message("Trade System Running", str(datetime.datetime.today()))


    '''
    This sends a formatted frame of all trade signals. Not all trade signals actually turn into orders though, depending
    on current USDT balance.
    '''
    if len(tradeSigList) > 0:
        orders = pd.DataFrame(tradeSigList)
        pushbullet_message("Trade Sigs\n", str(datetime.datetime.today()) + "\n" + str(orders.to_string()))

    '''
    This sends a formatted frame of all orders that were actually attempted.
    '''
    if len(ordersMade) > 0:
        orders = pd.DataFrame(ordersMade)
        orders.columns = ['Symbol', 'Side', 'Amount', 'Price', 'Status']
        pushbullet_message("Crypto Trades\n", str(datetime.datetime.today()) + "\n" + str(orders.to_string()))

    '''
    Finally, the number of failed orders is sent.
    '''
    if failedOrders > 0:
        pushbullet_message("Failed Orders\n", "Total failed orders = " + str(failedOrders))

    '''
    Finally, we delete the run token from the dynamoDB to free up the function for its next use.
    '''
    try:
        response = table.delete_item(Key={"RequestID": "portfolioSignals"})
    except:
        print("Dynamo Token not deleted.")


'''
AWS Lambda requires a lambdaHandler function call as a hook to start the function.
'''
if __name__ == "__main__":
    lambdaHandler(1, 2)