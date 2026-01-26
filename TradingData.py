from pandas import DataFrame
from Postgres import Postgres
from Token_model import Token


class TradingData:

    symbol = ""
    period = 0
    df = DataFrame()

    def __init__(self, db:Postgres, token:Token, period:int):
        df_temp = db.read_sql(f"SELECT klineopentime, openprice, lowprice, highprice, closeprice FROM token_historic WHERE symbol = '{token.name}' ORDER BY klineopentime ASC;")
        df_temp.index = df_temp["klineopentime"]
        self.df = df_temp
        self.period = period

    def calc_sma_list(self):
       self.df["sma"] = self.df["close"].rolling(window=self.period).mean()

    def calc_sma_number(self):
        last_index = self.df["sma"].last_valid_index()
        start = max(0, last_index - self.period + 1)
        self.df.loc[start:, "sma"] = (
            self.df["close"].iloc[start:].rolling(window=self.period).mean()
        )
