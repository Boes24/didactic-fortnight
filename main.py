import time
import pandas as pd
from Webserver.webserver import setup_logging, start_webserver
from check_and_execute import check_and_execute
from randomForest import train_ai_model
from Binance.binance import BinanceAPI
from Binance.wallet import Wallet
import Postgres
pd.set_option('display.float_format', lambda x: '%.2f' % x)

setup_logging()
start_webserver()

time_interval = "5m"
periods_to_sell = 18
db = Postgres.Postgres(time_interval)
db.init_database()
binance_wallet = Wallet(api=BinanceAPI(), db=db)

for t in binance_wallet.assets:
    db.init_table(api=binance_wallet.api, token=t, periods_to_sell=periods_to_sell)
    train_ai_model(db=db, token=t)


def task(retries: int = 0):
    for __token in binance_wallet.assets:
        # Fetch the last processed klineopentime for the token
        db_time = db.read_sql(f"SELECT klineopentime FROM token WHERE symbol = '{__token.name}' ORDER BY klineopentime DESC LIMIT 1;").iloc[0]["klineopentime"]

        # Update the token's data
        result = db.update_table(binance_wallet.api, __token)

        if result is None:
            # If the update fail, retry up to 5 times
            retries += 1
            if retries >= 5:
                print(f"Task ended after {retries} retries for token {__token.name}.")
                break
            print(f"Retrying task for {__token.name}...")
            time.sleep(1)  # Sleep to avoid spamming requests
            task(retries)  # Call again without recursion in the same context
            return  # exit the function so we don't continue the current loop

        else:
            retries = 0  # Reset retries on successful update

        check_and_execute(wallet=binance_wallet, db=db, token=__token, db_time=db_time, retries=0, periods_to_sell=periods_to_sell)


print("and now we wait...")
midnight_triggered = False

while True:
    if int(time.time()) % db.convert_countdown_to_seconds() == 1:
        task(0)
    time.sleep(0.4)