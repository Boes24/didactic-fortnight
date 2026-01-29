import pandas as pd
from Binance.binance import BinanceAPI
from Token_model import Token
from Postgres import Postgres


class Wallet:
    assets:list[Token] = []
    usdc_locked:float
    usdc_free:float
    api: BinanceAPI

    def __init__(self, api:BinanceAPI, db:Postgres):
        self.api = api
        token_metadata = db.get_tokens()
        account_data = self.api.get_wallet(token_metadata).balances
        for asset in account_data:
            if asset.asset == "USDC":
                self.usdc_locked = float(asset.locked)
                self.usdc_free = float(asset.free)
            else:
                self.assets.append(Token(name=asset.asset.lower(),
                                     price_decimals=token_metadata.loc[asset.asset.lower(), "price_decimals"],
                                     amount_decimals=token_metadata.loc[asset.asset.lower(), "quantity_decimals"],
                                     free=asset.free,
                                     locked=asset.locked))

    def update_wallet(self):
        df = pd.DataFrame({'index': sorted({a.name for a in self.assets})})
        account_data = self.api.get_wallet(df).balances
        
        token_index = {t.name : t for t in self.assets}

        for asset in account_data:
            token = token_index.get(asset.asset)

            if token:
                token.free = float(asset.free)
                token.locked = float(asset.locked)
            if asset.asset == "USDC":
                self.usdc_free = float(asset.free)
                self.usdc_locked = float(asset.locked)

