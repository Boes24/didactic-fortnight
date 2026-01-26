from binance_common.errors import BadRequestError
from binance_sdk_spot import rest_api
from binance_sdk_spot.rest_api.models import enums
from binance_common.configuration import ConfigurationRestAPI
from pandas import DataFrame
from urllib3.exceptions import ReadTimeoutError


class BinanceAPI:
    __binance_client = None

    Side = enums.NewOrderSideEnum
    Type = enums.NewOrderTypeEnum
    KlinesInterval = enums.KlinesIntervalEnum
    AboveType = enums.OrderListOcoAboveTypeEnum
    AboveTimeInForce = enums.OrderListOcoAboveTimeInForceEnum
    BelowType = enums.OrderListOcoBelowTypeEnum
    belowTimeInForce = enums.OrderListOcoBelowTimeInForceEnum

    def __init__(self):
        self.__binance_client = rest_api.SpotRestAPI(ConfigurationRestAPI(timeout=10000,
            api_key='M0AewqK5TjkHeqNuHWTi9i88eY2NVDYRkP77vaxOZc7T2UPeneFFzyTB5Ik4smJv',
            api_secret='2saH7pPndppjn93a0DcOq5aXCDHb9pIFqyACqZqZtKdSsuigCpcFk2TzH0nwQaAp',
            base_path='https://api.binance.com')
        )
        try:
            print("Server time:", self.__binance_client.time().data().server_time)
        except BadRequestError as e:
            print("Couldn't get time:", e)

    def get_wallet(self, token_metadata:DataFrame):
        data = self.__binance_client.get_account().data()
        # Brug DataFrame-symboler som whitelist
        valid_symbols = set(token_metadata.index)
        valid_symbols.add('usdc')

        data.balances = [
            b for b in data.balances
            if b.asset.lower() in valid_symbols
        ]
        return data

    def new_order(self, symbol:str, side:enums.NewOrderSideEnum, type:enums.NewOrderTypeEnum, quantity:float, price:float=None):
        try:
            return self.__binance_client.new_order(symbol=symbol.upper(), side=side, type=type, quantity=quantity, price=price)
        except BadRequestError as e:
            print(f"Binance rejected order: {e}")
        except Exception as e:
            print(f"Unexpected error placing order: {e}")

    def new_order_oco(self, symbol:str, side:enums.NewOrderSideEnum, quantity:float, aboveType:enums.OrderListOcoAboveTypeEnum, abovePrice:float, belowType:enums.OrderListOcoBelowTypeEnum, belowStopPrice:float):
        try:
            return self.__binance_client.order_list_oco(symbol=symbol, side=side, quantity=quantity, above_type=aboveType, above_price=abovePrice, below_type=belowType, below_stop_price=belowStopPrice)
        except BadRequestError as e:
            print(f"Binance rejected order: {e}")
        except Exception as e:
            print(f"Unexpected error placing order: {e}")


    def get_candlesticks(self, symbol: str, interval: enums.KlinesIntervalEnum, limit: int = 1000, start_time: int | None = None, end_time: int | None = None ):
        params = {
            "symbol": (symbol + 'usdc').upper(),
            "interval": interval,
            "limit": limit
        }
        # Only include optional parameters if they are provided
        if start_time is not None:
            params["start_time"] = start_time * 1000
        if end_time is not None:
            params["end_time"] = end_time * 1000
        try:
            return self.__binance_client.klines(**params)
        except BadRequestError as e:
            print(f"Couldn't get candlesticks: {e}")
        except ReadTimeoutError as e:
            print(f"Couldn't get candlesticks: {e}")

    def test_new_order(self, symbol:str, side:enums.NewOrderSideEnum, type:enums.NewOrderTypeEnum, quantity:float):
        return self.__binance_client.order_test(symbol=symbol.upper(), side=side, type=type, quantity=quantity)