# LambdaTrade
A simple python script for automated cryptocurrency trading on Binance. Intended to be deployed on AWS.

Currently the script is set up to perform a simple MACD momentum trade on weekly bars, but it is quite easily adapted for any arbitrary logic. It is also currently setup to trade USDT pairs by watching a tickerList saved in Dropbox. This allows for an easily updateable watchlist without having to interact with AWS directly. Finally, Pushbullet is used to inform the user of executions, as well as any interactions that occure with the Binance exchange.

For those that are new to AWS, this script can be triggered automatically at predetermined times using CloudWatch.
