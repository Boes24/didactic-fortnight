from time import sleep
import pandas as pd
from binance_sdk_spot.rest_api.models import NewOrderResponse
from Binance.wallet import Wallet
from Token_model import Token
from Postgres import Postgres
sleep_duration = 2


def check_and_execute(wallet:Wallet, db: Postgres, token: Token, db_time: int, retries: int, periods_to_sell:int):

    ##Check for a buy order
    latest_trading_data = db.get_latest_trading_data(token)
    latest_time = latest_trading_data["klineopentime"]
    latest_trading_data.drop("klineopentime", inplace=True)

    if latest_time != db_time:
        row: pd.DataFrame = latest_trading_data.to_frame().T
        if len(row) == 1:
            prediction = token.model.predict(row)
            if prediction[0] == 1:
                close_price: float = float(row["closeprice"].iloc[0])
                print(f"Buy {token.name} at {latest_time + db.convert_countdown_to_seconds()}\n - closing price: {close_price}")
                wallet.update_wallet()
                print(f"There is {wallet.usdc_free} usdc free in wallet")
                if wallet.usdc_free < 10:
                    print(f"There is only {wallet.usdc_free} USDC free coins in your wallet")
                    return
                response = wallet.api.new_order(
                    symbol=token.name.upper()+"USDC", side=wallet.api.Side.BUY,
                    type=wallet.api.Type.MARKET,
                    quantity=round(wallet.usdc_free * 0.8 / close_price, token.quantity_decimals))
                try:
                    new_order_response: NewOrderResponse = response.data()
                    print(f"response_data: '{new_order_response}'")
                except Exception as e:
                    new_order_response = None
                    print("Exception while getting response from buy order", e)

                if new_order_response is not None:
                    amount_bought:float = 0.0
                    all_fills_avg_price:float = 0.0
                    commission:float = 0.0
                    for fill in new_order_response.fills:
                        amount_bought += float(fill.qty)
                        print(f"amount_bought: {amount_bought}")

                        if fill.commission_asset == token.name.upper():
                            commission += float(fill.commission)

                        all_fills_avg_price += float(fill.price) * float(fill.qty)
                        print(f"all_fills_avg_price: {all_fills_avg_price}")

                    all_fills_avg_price = all_fills_avg_price / amount_bought
                    amount_bought -= commission
                    print("Amount bought after commission",amount_bought)
                    print("avg price:", all_fills_avg_price)
                    rsp = wallet.api.new_order_oco(
                        symbol=token.name.upper()+"USDC",
                        side=wallet.api.Side.SELL,
                        quantity=round(amount_bought*0.99, token.quantity_decimals),
                        aboveType=wallet.api.AboveType.LIMIT_MAKER,
                        abovePrice=round(all_fills_avg_price*1.015, token.price_decimals),
                        belowType=wallet.api.BelowType.STOP_LOSS,
                        belowStopPrice=round(all_fills_avg_price*0.975, token.price_decimals))
                    print(f"response_data: '{rsp.data().order_reports}'")
                else:
                    print("response from buy order is None")
        else:
            print("Not all columns have a value - try again")
            sleep(sleep_duration)
            check_and_execute(wallet=wallet, db=db, token=token, db_time=db_time, retries=retries, periods_to_sell=periods_to_sell)
    else:
        print("Still no new data - try again")
        retries += 1
        if retries < 3:
            sleep(sleep_duration)
            check_and_execute(wallet=wallet,db=db, token=token, db_time=db_time, retries=retries, periods_to_sell=periods_to_sell)
