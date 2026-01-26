import time
from decimal import Decimal
from binance_common.models import ApiResponse
from binance_sdk_spot.rest_api.models import KlinesResponse
from sqlalchemy import create_engine, text
import pandas as pd
from Token_model import Token
from Binance.binance import BinanceAPI
from dotenv import load_dotenv
import os


def truncate_decimals(n, decimals):
    return Decimal(n).quantize(Decimal(10) ** -decimals, rounding='ROUND_DOWN')



class Postgres:

    def __init__(self, time_interval):
        self.time_interval = time_interval
        load_dotenv()
        user = os.environ["DATABASE_USER"]
        password = os.environ["DATABASE_PASSWORD"]
        host = os.environ["DATABASE_HOST"]
        port = os.environ.get("DATABASE_PORT", "5432")

        self.engine = create_engine(f"postgresql+psycopg://{user}:{password}@{host}:{port}/{self.time_interval}")

    def init_database(self):
        self.__create_table__()
        self.__create_table_goodbuytime__()
        self.__create_table_sma__()
        self.__create_table_rsi__()
        self.__create_table_vwap__()
        self.__create_table_ema__()
        self.__create_functions__()
        self.__create_indexes()
        #self.__create_table_change_n_range()

    def init_table(self, api, token:Token, periods_to_sell:int):
        self.__init_table_with_data__(api, token)
        if self.get_latest_trading_data(token) is None:
            self.__init_good_buy_time__(token, periods_to_sell)
            self.__init_table_sma__(token)
            self.__init_table_ema__(token)
            self.__init_table_rsi__(token)
            self.__init_table_vwap__(token)
            #self.__init_change_n_range(token)
            self.execute_query(f"INSERT INTO token SELECT * FROM token_historic WHERE symbol = '{token.name}' ORDER BY klineopentime DESC LIMIT 1;")
        else:
            self.execute_query(f"TRUNCATE TABLE token;")
            self.execute_query(f"INSERT INTO token SELECT * FROM token_historic WHERE symbol = '{token.name}' ORDER BY klineopentime DESC LIMIT 1;")


    def read_sql(self, sql:str):
        return pd.read_sql(sql, self.engine)

    def execute_query(self, query):
        with self.engine.begin() as conn:
            return conn.execute(text(query))

    def get_tokens(self):
        sql = "SELECT symbol, price_decimals,quantity_decimals FROM token_metadata WHERE active = True ORDER BY symbol asc"
        df = pd.read_sql(sql, self.engine, coerce_float=False)
        df = df.set_index('symbol')
        return df

    def read_table_historic(self, token:Token):
        sql = f''' 
                SELECT
                    th.klineopentime, th.lowprice, th.highprice, th.closeprice, th.volume,
                    token_sma."50"  AS "sma 50",
                    token_sma."100" AS "sma 100",
                    token_sma."200" AS "sma 200",
                    token_sma."500" AS "sma 500",
                
                    token_ema."50"  AS "ema 50",
                    token_ema."100" AS "ema 100",
                    token_ema."200" AS "ema 200",
                    token_ema."500" AS "ema 500",
                
                    token_rsi."50"  AS "rsi 50",
                    token_rsi."100" AS "rsi 100",
                    token_rsi."200" AS "rsi 200",
                    token_rsi."500" AS "rsi 500",
                
                    token_vwap."50"  AS "vwap 50",
                    token_vwap."100" AS "vwap 100",
                    token_vwap."200" AS "vwap 200",
                    token_vwap."500" AS "vwap 500",
                    token_goodbuytime.good_buy AS "goodbuytime"
                   
                
                FROM public.token_historic th
                
                JOIN token_sma ON th.klineopentime = token_sma.klineopentime AND th.symbol = token_sma.symbol
                JOIN token_ema ON th.klineopentime = token_ema.klineopentime AND th.symbol = token_ema.symbol
                JOIN token_rsi ON th.klineopentime = token_rsi.klineopentime AND th.symbol = token_rsi.symbol
                JOIN token_vwap ON th.klineopentime = token_vwap.klineopentime AND th.symbol = token_vwap.symbol
                JOIN token_goodbuytime ON th.klineopentime = token_goodbuytime.klineopentime AND th.symbol = token_goodbuytime.symbol
                --JOIN change_n_range ON th.klineopentime = change_n_range.klineopentime  AND th.symbol = change_n_range.symbol
                
                WHERE th.symbol = '{token.name}'
                ORDER BY th.klineopentime ASC;
                '''
        return pd.read_sql(sql, self.engine, coerce_float=False)

    def get_latest_trading_data(self, token:Token):
        sql = f"SELECT * FROM get_latest_trading_data('{token.name}');"
        try:
            return pd.read_sql(sql, self.engine, coerce_float=False).iloc[0]
        except Exception as e:
            print("No data found in get_latest_trading_data. Error:", e)
            return None

    def update_table(self, api:BinanceAPI, token:Token, table_name:str = "token"):
        start_time = int(self.read_sql(f"select klineopentime from {table_name} where symbol = '{token.name}' order by klineopentime desc limit 1;").iloc[0]["klineopentime"])
        try:
            response:ApiResponse[KlinesResponse] = api.get_candlesticks(symbol=token.name, interval=api.KlinesInterval(self.time_interval), start_time=start_time)
        except Exception as e:
            print("No data found in get_candlesticks. Error:", e)
            return None

        klines = response.data()
        if len(klines) < 3:
            return None
        klines = klines[1:-1]
        values_sql = ", ".join(
            f"('{token.name}',"
            f"{int(str(stick[0])[:-3])}, {truncate_decimals(stick[1], token.price_decimals)}, {truncate_decimals(stick[2],token.price_decimals)},"
            f"{truncate_decimals(stick[3], token.price_decimals)}, {truncate_decimals(stick[4], token.price_decimals)},"
            f"{truncate_decimals(stick[5], 2)}, {truncate_decimals(stick[7], 5)}, {int(stick[8])},"
            f"{truncate_decimals(stick[9], 1)}, {truncate_decimals(stick[10], 5)})"
            for stick in klines
        )
        with self.engine.begin() as conn:
            return conn.execute(text(f"INSERT INTO public.{table_name} VALUES {values_sql};"))


    def __create_functions__(self):
        print("Create functions")
        with self.engine.begin() as conn:
            sql = f'''
                CREATE OR REPLACE FUNCTION public.get_latest_trading_data(
                symbol_name character varying(6))
                RETURNS TABLE(
                klineopentime integer,
                lowprice numeric,
                highprice numeric,
                closeprice numeric,
                volume numeric,
                "sma 50" numeric,
                "sma 100" numeric,
                "sma 200" numeric,
                "sma 500" numeric,
                "ema 50" numeric,
                "ema 100" numeric,
                "ema 200" numeric,
                "ema 500" numeric,
                "rsi 50" numeric,
                "rsi 100" numeric,
                "rsi 200" numeric,
                "rsi 500" numeric,
                "vwap 50" numeric,
                "vwap 100" numeric,
                "vwap 200" numeric,
                "vwap 500" numeric
                ) 
                LANGUAGE 'plpgsql'
                COST 100
                VOLATILE PARALLEL UNSAFE
                ROWS 1000
            
                AS $BODY$
                DECLARE
                sql text;
                BEGIN
                sql := format($f$
                SELECT
                token.klineopentime,
                token.lowprice,
                token.highprice,
                token.closeprice,
                token.volume,
                token_sma."50"  AS "sma 50",
                token_sma."100" AS "sma 100",
                token_sma."200" AS "sma 200",
                token_sma."500" AS "sma 500",
                token_ema."50"  AS "ema 50",
                token_ema."100" AS "ema 100",
                token_ema."200" AS "ema 200",
                token_ema."500" AS "ema 500",
                token_rsi."50"  AS "rsi 50",
                token_rsi."100" AS "rsi 100",
                token_rsi."200" AS "rsi 200",
                token_rsi."500" AS "rsi 500",
                token_vwap."50"  AS "vwap 50",
                token_vwap."100" AS "vwap 100",
                token_vwap."200" AS "vwap 200",
                token_vwap."500" AS "vwap 500"
                FROM public.token
                JOIN token_sma ON token.klineopentime = token_sma.klineopentime AND token.symbol = token_sma.symbol
                JOIN token_ema ON token.klineopentime = token_ema.klineopentime AND token.symbol = token_ema.symbol
                JOIN token_rsi ON token.klineopentime = token_rsi.klineopentime AND token.symbol = token_rsi.symbol
                JOIN token_vwap ON token.klineopentime = token_vwap.klineopentime AND token.symbol = token_vwap.symbol
                WHERE token.symbol = '%1$I'
                ORDER BY token.klineopentime DESC
                LIMIT 1
                $f$, symbol_name);
                RETURN QUERY EXECUTE sql;
                END;        
                $BODY$;
                
                ALTER FUNCTION public.get_latest_trading_data(character varying)
                OWNER TO admin;
                '''
            conn.execute(text(sql))

            sql = f"""
                CREATE OR REPLACE FUNCTION insert_sma_on_insert()
                RETURNS TRIGGER
                LANGUAGE plpgsql
                AS $$
                DECLARE
                    token_decimals INTEGER;
                    sma50 NUMERIC;
                    sma100 NUMERIC;
                    sma200 NUMERIC;
                    sma500 NUMERIC;
                BEGIN
                    -- Hent antal decimals for symbol
                    SELECT price_decimals INTO token_decimals
                    FROM token_metadata
                    WHERE symbol = NEW.symbol;
                
                    -- Beregn SMA for den nye række kun
                    SELECT
                        ROUND(AVG(closeprice) OVER (ORDER BY klineopentime ROWS 49 PRECEDING), token_decimals),
                        ROUND(AVG(closeprice) OVER (ORDER BY klineopentime ROWS 99 PRECEDING), token_decimals),
                        ROUND(AVG(closeprice) OVER (ORDER BY klineopentime ROWS 199 PRECEDING), token_decimals),
                        ROUND(AVG(closeprice) OVER (ORDER BY klineopentime ROWS 499 PRECEDING), token_decimals)
                    INTO sma50, sma100, sma200, sma500
                    FROM token_historic
                    WHERE klineopentime <= NEW.klineopentime AND symbol = NEW.symbol
                    ORDER BY klineopentime DESC
                    LIMIT 1;
                
                    -- Indsæt kun for den nye klineopentime
                    INSERT INTO token_sma(symbol, klineopentime, "50", "100", "200", "500")
                    VALUES (NEW.symbol, NEW.klineopentime, sma50, sma100, sma200, sma500)
                    ON CONFLICT (symbol, klineopentime) DO NOTHING;
                
                    RETURN NEW;
                END;
                $$;
                
                -- Triggeren:
                DROP TRIGGER IF EXISTS trg_insert_sma ON token;
                CREATE TRIGGER trg_insert_sma
                AFTER INSERT ON token
                FOR EACH ROW
                EXECUTE FUNCTION insert_sma_on_insert();
                """
            conn.execute(text(sql))

            sql = f"""
                CREATE OR REPLACE FUNCTION insert_ema_on_insert()
                RETURNS TRIGGER
                LANGUAGE plpgsql
                AS $$
                DECLARE
                    periods numeric[] := ARRAY[50,100,200,500];
                    mults numeric[] := ARRAY[2.0/(50+1), 2.0/(100+1), 2.0/(200+1), 2.0/(500+1)];
                    p INTEGER;
                    ema_vals numeric[] := ARRAY[NULL,NULL,NULL,NULL]; -- EMA50,100,200,500
                    avg_val numeric;
                    token_decimals INTEGER;
                BEGIN
                    -- Hent antal decimals for symbol
                    SELECT price_decimals INTO token_decimals
                    FROM token_metadata
                    WHERE symbol = NEW.symbol;
                
                    -- Hent seneste EMA som array (kun én række)
                    SELECT ARRAY["50","100","200","500"]
                    INTO ema_vals
                    FROM token_ema
                    WHERE symbol = NEW.symbol
                    ORDER BY klineopentime DESC
                    LIMIT 1;
                
                    -- Beregn ny EMA for hver periode
                    FOR p IN 1..array_length(periods,1) LOOP
                        -- Klassisk EMA-formel
                        ema_vals[p] := (NEW.closeprice - ema_vals[p]) * mults[p] + ema_vals[p];
                    END LOOP;
                
                    -- Insert kun for den nye klineopentime
                    INSERT INTO token_ema(symbol, klineopentime, "50", "100", "200", "500")
                    VALUES (NEW.symbol, NEW.klineopentime,
                            ROUND(ema_vals[1], token_decimals),
                            ROUND(ema_vals[2], token_decimals),
                            ROUND(ema_vals[3], token_decimals),
                            ROUND(ema_vals[4], token_decimals))
                    ON CONFLICT (symbol, klineopentime) DO NOTHING;
                
                    RETURN NEW;
                END;
                $$;
                
                -- Trigger
                DROP TRIGGER IF EXISTS trg_insert_ema ON token;
                CREATE TRIGGER trg_insert_ema
                AFTER INSERT ON token
                FOR EACH ROW
                EXECUTE FUNCTION insert_ema_on_insert();
                """
            conn.execute(text(sql))

            sql = f"""
            CREATE OR REPLACE FUNCTION insert_rsi_on_insert()
            RETURNS TRIGGER
            LANGUAGE plpgsql
            AS $$
            DECLARE
                periods INTEGER[] := ARRAY[50,100,200,500];
                p INTEGER;
                gains NUMERIC;
                losses NUMERIC;
                avg_gain NUMERIC;
                avg_loss NUMERIC;
                profit NUMERIC;
                close_prev NUMERIC;
                close_curr NUMERIC;
                rsi NUMERIC;
                rsi_values NUMERIC[4];
                idx INTEGER := 1;
                token_decimals INTEGER;
            BEGIN
                -- Hent antal decimals
                SELECT price_decimals INTO token_decimals
                FROM token_metadata
                WHERE symbol = NEW.symbol;
            
                -- Hent forrige closepris for symbolet
                SELECT closeprice INTO close_prev
                FROM token_historic
                WHERE klineopentime < NEW.klineopentime
                  AND symbol = NEW.symbol
                ORDER BY klineopentime DESC
                LIMIT 1;
            
                close_curr := NEW.closeprice;
                profit := close_curr - COALESCE(close_prev, close_curr); -- Hvis ingen forrige pris, profit = 0
            
                FOREACH p IN ARRAY periods LOOP
                    -- Hent seneste avg_gain/avg_loss
                    SELECT s.avg_gain, s.avg_loss
                    INTO avg_gain, avg_loss
                    FROM rsi_state s
                    WHERE s.symbol = NEW.symbol AND s.period = p
                    ORDER BY klineopentime DESC
                    LIMIT 1;
            
                    -- Beregn gain/loss
                    gains := GREATEST(profit, 0);
                    losses := GREATEST(-profit, 0);
            
                    -- Opdater glidende gennemsnit
                    avg_gain := (COALESCE(avg_gain, 0) * (p - 1) + gains) / p;
                    avg_loss := (COALESCE(avg_loss, 0) * (p - 1) + losses) / p;
            
                    -- Beregn RSI
                    IF avg_loss = 0 THEN
                        rsi := 100;
                    ELSE
                        rsi := ROUND((100 - (100 / (1 + (avg_gain / avg_loss))))::numeric, token_decimals);
                    END IF;
            
                    rsi_values[idx] := rsi;
            
                    -- Indsæt ny RSI-state
                    INSERT INTO rsi_state(symbol, period, klineopentime, avg_gain, avg_loss)
                    VALUES (NEW.symbol, p, NEW.klineopentime, avg_gain, avg_loss) ON CONFLICT DO NOTHING;
            
                    idx := idx + 1;
                END LOOP;
            
                -- Indsæt i token_rsi
                INSERT INTO token_rsi(symbol, klineopentime, "50", "100", "200", "500")
                VALUES (NEW.symbol, NEW.klineopentime,
                        rsi_values[1],
                        rsi_values[2],
                        rsi_values[3],
                        rsi_values[4])
                ON CONFLICT (symbol, klineopentime) DO NOTHING;
            
                RETURN NEW;
            END;
            $$;
            
            -- Trigger
            DROP TRIGGER IF EXISTS trg_insert_rsi ON token;
            CREATE TRIGGER trg_insert_rsi
            AFTER INSERT ON token
            FOR EACH ROW
            EXECUTE FUNCTION insert_rsi_on_insert();
            """
            conn.execute(text(sql))
            sql = f"""
                CREATE OR REPLACE FUNCTION insert_vwap_on_insert()
                RETURNS TRIGGER
                LANGUAGE plpgsql
                AS $$
                DECLARE
                    token_decimals INTEGER;
                    vwap50 NUMERIC;
                    vwap100 NUMERIC;
                    vwap200 NUMERIC;
                    vwap500 NUMERIC;
                BEGIN
                    -- Hent antal decimals for symbolet
                    SELECT price_decimals INTO token_decimals
                    FROM token_metadata
                    WHERE symbol = NEW.symbol;
                
                    -- Beregn VWAP for hver periode
                    SELECT
                        SUM(closeavg * volume) FILTER (WHERE rn <= 50)  / SUM(volume) FILTER (WHERE rn <= 50)  AS vwap50,
                        SUM(closeavg * volume) FILTER (WHERE rn <= 100) / SUM(volume) FILTER (WHERE rn <= 100) AS vwap100,
                        SUM(closeavg * volume) FILTER (WHERE rn <= 200) / SUM(volume) FILTER (WHERE rn <= 200) AS vwap200,
                        SUM(closeavg * volume) FILTER (WHERE rn <= 500) / SUM(volume) FILTER (WHERE rn <= 500) AS vwap500
                    INTO vwap50, vwap100, vwap200, vwap500
                    FROM (
                        SELECT
                            (highprice + lowprice + closeprice) / 3 AS closeavg,
                            volume,
                            ROW_NUMBER() OVER (ORDER BY klineopentime DESC) AS rn
                        FROM token_historic
                        WHERE symbol = NEW.symbol
                          AND klineopentime <= NEW.klineopentime
                        ORDER BY klineopentime DESC
                        LIMIT 500
                    ) sub;

                
                    -- Insert den nye VWAP
                    INSERT INTO token_vwap(symbol, klineopentime, "50", "100", "200", "500")
                    VALUES (
                        NEW.symbol,
                        NEW.klineopentime,
                        ROUND(vwap50, token_decimals),
                        ROUND(vwap100, token_decimals),
                        ROUND(vwap200, token_decimals),
                        ROUND(vwap500, token_decimals)
                    )
                    ON CONFLICT (symbol, klineopentime) DO NOTHING;
                
                    RETURN NEW;
                END;
                $$;
                
                -- Trigger
                DROP TRIGGER IF EXISTS trg_insert_vwap ON token;
                CREATE TRIGGER trg_insert_vwap
                AFTER INSERT ON token
                FOR EACH ROW
                EXECUTE FUNCTION insert_vwap_on_insert();
                """

            conn.execute(text(sql))

    def __create_indexes(self):
        sql = f"""
        CREATE INDEX IF NOT EXISTS token_historic_symbol_kline_idx ON token_historic (symbol, klineopentime);
        CREATE INDEX IF NOT EXISTS token_historic_symbol_kline_idx_desc ON token_historic (symbol, klineopentime desc);
        CREATE INDEX IF NOT EXISTS token_sma_symbol_kline_idx ON token_sma (symbol, klineopentime);
        CREATE INDEX IF NOT EXISTS token_ema_symbol_kline_idx ON token_ema (symbol, klineopentime);
        CREATE INDEX IF NOT EXISTS token_rsi_symbol_kline_idx ON token_vwap (symbol, klineopentime);
        CREATE INDEX IF NOT EXISTS token_vwap_symbol_kline_idx ON token_rsi (symbol, klineopentime);
        CREATE INDEX IF NOT EXISTS token_goodbuytime_symbol_kline_idx ON token_goodbuytime (symbol, klineopentime);
        """
        self.execute_query(sql)

    def __create_table__(self):
        print(f"create table 'token'")
        with self.engine.begin() as conn:
            sql = f"""
                CREATE TABLE IF NOT EXISTS public.token_metadata(
                    symbol character varying(6) COLLATE pg_catalog."default" NOT NULL,
                    price_decimals integer NOT NULL,
                    quantity_decimals integer,
                    CONSTRAINT token_metadata_pkey PRIMARY KEY (symbol)
                );
                CREATE INDEX IF NOT EXISTS idx_symbol ON token_metadata(symbol DESC);
                """
            conn.execute(text(sql))

            sql = f'''
            CREATE TABLE IF NOT EXISTS public.token(
                symbol character varying(6) NOT NULL,
                klineopentime integer NOT NULL,
                openprice numeric NOT NULL,
                highprice numeric NOT NULL,
                lowprice numeric NOT NULL,
                closeprice numeric NOT NULL,
                volume numeric NOT NULL,
                quoteassetvolume numeric NOT NULL,
                numberoftrades integer NOT NULL,
                takerbuybaseassetvolume numeric NOT NULL,
                takerbuyquoteassetvolume numeric NOT NULL,
                
                CONSTRAINT "token_pkey" PRIMARY KEY (symbol, klineopentime),
                CONSTRAINT symbol FOREIGN KEY (symbol)
                REFERENCES public.token_metadata (symbol) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION
                );
                CREATE INDEX IF NOT EXISTS idx_token_klineopentime_desc ON public.token (symbol, klineopentime);
            '''
            conn.execute(text(sql))

            sql = f'''
            CREATE TABLE IF NOT EXISTS public.token_historic(
                symbol character varying(6) NOT NULL,
                klineopentime integer NOT NULL,
                openprice numeric NOT NULL,
                highprice numeric NOT NULL,
                lowprice numeric NOT NULL,
                closeprice numeric NOT NULL,
                volume numeric NOT NULL,
                quoteassetvolume numeric NOT NULL,
                numberoftrades integer NOT NULL,
                takerbuybaseassetvolume numeric NOT NULL,
                takerbuyquoteassetvolume numeric NOT NULL,
                CONSTRAINT "token_historic_pkey" PRIMARY KEY (symbol, klineopentime),
                CONSTRAINT symbol FOREIGN KEY (symbol)
                REFERENCES public.token_metadata (symbol) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION
                );
                CREATE INDEX IF NOT EXISTS idx_historic_cover
                ON token_historic (symbol, klineopentime)
                INCLUDE (lowprice, highprice, closeprice, volume);

            
            CREATE OR REPLACE FUNCTION insert_into_token_historic()
            RETURNS TRIGGER AS $$
            BEGIN
                INSERT INTO token_historic VALUES (NEW.symbol, NEW.klineopentime, NEW.openprice, NEW.highprice, NEW.lowprice, NEW.closeprice, NEW.volume, NEW.quoteassetvolume, NEW.numberoftrades, NEW.takerbuybaseassetvolume, NEW.takerbuyquoteassetvolume)
                ON CONFLICT DO NOTHING;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            
            DROP TRIGGER IF EXISTS aaa_trg_insert_token_to_historic ON token;
            CREATE TRIGGER aaa_trg_insert_token_to_historic
            AFTER INSERT ON token
            FOR EACH ROW
            EXECUTE FUNCTION insert_into_token_historic();
            '''
            conn.execute(text(sql))

    def __create_table_goodbuytime__(self):
        print(f"Create token_goodbuytime table ")
        with self.engine.begin() as conn:
            sql = f"""
            CREATE TABLE IF NOT EXISTS public.token_goodbuytime(
                symbol character varying(6),
                klineopentime integer,
                closeprice numeric,
                good_buy integer,
                CONSTRAINT token_goodbuytime_pkey PRIMARY KEY (symbol, klineopentime),
                CONSTRAINT symbol_klineopentime FOREIGN KEY (symbol, klineopentime)
                REFERENCES public.token_historic (symbol, klineopentime) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION
            );
            """
            conn.execute(text(sql))

    def __init_good_buy_time__(self, token:Token, periods_to_sell:int):
        with self.engine.begin() as conn:
            print("Calculate good buy time")
            if self.read_sql(f"select * from public.token_goodbuytime where symbol = '{token.name}';").shape[0] == 0:
                sql = f"""
                    INSERT INTO token_goodbuytime (symbol, klineopentime, closeprice, good_buy)
                    SELECT symbol,
                           klineopentime,
                           closeprice,
                           CASE
                               WHEN future_price >= closeprice * 1.015 THEN 1
                               ELSE 0
                           END
                    FROM (
                        SELECT symbol, klineopentime, closeprice,
                        LEAD(closeprice, {periods_to_sell}) OVER (PARTITION BY symbol ORDER BY klineopentime) AS future_price
                        FROM token_historic
                        WHERE symbol = '{token.name}'
                        order by klineopentime desc
                    ) t
                    WHERE future_price IS NOT NULL
                    ON CONFLICT DO NOTHING;
                    """
                conn.execute(text(sql))

    def __create_table_change_n_range(self):
        print(f"Create token_change_n_range table ")
        with self.engine.begin() as conn:
            sql = f"""
            CREATE TABLE IF NOT EXISTS public.change_n_range(
                symbol character varying(6),
                klineopentime integer,
                change_0 numeric,
                change_1 numeric,
                change_2 numeric,
                change_3 numeric,
                change_4 numeric,
                change_5 numeric,
                change_6 numeric,
                change_7 numeric,
                change_8 numeric,
                change_9 numeric,
                range_0 numeric,
                range_1 numeric,
                range_2 numeric,
                range_3 numeric,
                range_4 numeric,
                range_5 numeric,
                range_6 numeric,
                range_7 numeric,
                range_8 numeric,
                range_9 numeric,
                CONSTRAINT change_n_range_Pkey PRIMARY KEY (symbol, klineopentime),
                CONSTRAINT change_n_range_Fkey FOREIGN KEY (symbol, klineopentime)
                REFERENCES public.token_historic (symbol, klineopentime) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION
            );
            """
            conn.execute(text(sql))

    def __init_change_n_range(self, token:Token):
        with self.engine.begin() as conn:
            sql = f"""
                WITH base AS (
                    SELECT
                        symbol,
                        klineopentime,
                        closeprice,
                        openprice,
                        lowprice,
                        highprice,
                        ((closeprice - openprice) / openprice) * 100 AS change_0,
                        ((highprice - lowprice) / lowprice) * 100 AS range_0
                    FROM token_historic
                    WHERE symbol = '{token.name}'
                ),
                calc AS (
                    SELECT
                        b.symbol,
                        b.klineopentime,
                        b.change_0,
                        b.range_0,
                
                        -- change_1 til change_9
                        ((b.closeprice - lag(b.openprice, 1) OVER w) / lag(b.openprice, 1) OVER w) * 100 AS change_1,
                        ((b.closeprice - lag(b.openprice, 2) OVER w) / lag(b.openprice, 2) OVER w) * 100 AS change_2,
                        ((b.closeprice - lag(b.openprice, 3) OVER w) / lag(b.openprice, 3) OVER w) * 100 AS change_3,
                        ((b.closeprice - lag(b.openprice, 4) OVER w) / lag(b.openprice, 4) OVER w) * 100 AS change_4,
                        ((b.closeprice - lag(b.openprice, 5) OVER w) / lag(b.openprice, 5) OVER w) * 100 AS change_5,
                        ((b.closeprice - lag(b.openprice, 6) OVER w) / lag(b.openprice, 6) OVER w) * 100 AS change_6,
                        ((b.closeprice - lag(b.openprice, 7) OVER w) / lag(b.openprice, 7) OVER w) * 100 AS change_7,
                        ((b.closeprice - lag(b.openprice, 8) OVER w) / lag(b.openprice, 8) OVER w) * 100 AS change_8,
                        ((b.closeprice - lag(b.openprice, 9) OVER w) / lag(b.openprice, 9) OVER w) * 100 AS change_9,
                
                        -- range_1 til range_9
                        ((b.highprice - lag(b.lowprice, 1) OVER w) / lag(b.lowprice, 1) OVER w) * 100 AS range_1,
                        ((b.highprice - lag(b.lowprice, 2) OVER w) / lag(b.lowprice, 2) OVER w) * 100 AS range_2,
                        ((b.highprice - lag(b.lowprice, 3) OVER w) / lag(b.lowprice, 3) OVER w) * 100 AS range_3,
                        ((b.highprice - lag(b.lowprice, 4) OVER w) / lag(b.lowprice, 4) OVER w) * 100 AS range_4,
                        ((b.highprice - lag(b.lowprice, 5) OVER w) / lag(b.lowprice, 5) OVER w) * 100 AS range_5,
                        ((b.highprice - lag(b.lowprice, 6) OVER w) / lag(b.lowprice, 6) OVER w) * 100 AS range_6,
                        ((b.highprice - lag(b.lowprice, 7) OVER w) / lag(b.lowprice, 7) OVER w) * 100 AS range_7,
                        ((b.highprice - lag(b.lowprice, 8) OVER w) / lag(b.lowprice, 8) OVER w) * 100 AS range_8,
                        ((b.highprice - lag(b.lowprice, 9) OVER w) / lag(b.lowprice, 9) OVER w) * 100 AS range_9
                
                    FROM base b
                    WINDOW w AS (PARTITION BY b.symbol ORDER BY b.klineopentime)
                )
                INSERT INTO public.change_n_range (
                    symbol, klineopentime,
                    change_0, change_1, change_2, change_3, change_4, change_5, change_6, change_7, change_8, change_9,
                    range_0, range_1, range_2, range_3, range_4, range_5, range_6, range_7, range_8, range_9
                )
                SELECT
                    symbol,
                    klineopentime,
                    TRUNC(change_0, 2),
                    TRUNC(change_1, 2),
                    TRUNC(change_2, 2),
                    TRUNC(change_3, 2),
                    TRUNC(change_4, 2),
                    TRUNC(change_5, 2),
                    TRUNC(change_6, 2),
                    TRUNC(change_7, 2),
                    TRUNC(change_8, 2),
                    TRUNC(change_9, 2),
                    TRUNC(range_0, 2),
                    TRUNC(range_1, 2),
                    TRUNC(range_2, 2),
                    TRUNC(range_3, 2),
                    TRUNC(range_4, 2),
                    TRUNC(range_5, 2),
                    TRUNC(range_6, 2),
                    TRUNC(range_7, 2),
                    TRUNC(range_8, 2),
                    TRUNC(range_9, 2)
                FROM calc
            """
            conn.execute(text(sql))

    def __create_table_sma__(self):
        print(f"Create token_sma table ")
        with self.engine.begin() as conn:
            sql = f"""
            CREATE TABLE IF NOT EXISTS public.token_sma(
                symbol character varying(6) NOT NULL,
                klineopentime integer NOT NULL,
                "50" numeric,
                "100" numeric,
                "200" numeric,
                "500" numeric,
                CONSTRAINT "token_sma_pkey" PRIMARY KEY (symbol, klineopentime),
                CONSTRAINT symbol_klineopentime FOREIGN KEY (symbol, klineopentime)
                REFERENCES public.token_historic (symbol, klineopentime) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION);
            """
            conn.execute(text(sql))

    def __init_table_sma__(self, token:Token):
        print(f"Init {token.name} sma")
        if self.read_sql(f"select * from public.token_sma where symbol = '{token.name}';").shape[0] == 0:
            with self.engine.begin() as conn:
                sql = f"""
                INSERT INTO token_sma (
                symbol,
                klineopentime,
                "50",
                "100",
                "200",
                "500"
                )
                SELECT
                    '{token.name}' AS symbol,
                    klineopentime,
                    ROUND(AVG(closeprice) OVER (ORDER BY klineopentime ROWS 49 PRECEDING), {token.price_decimals})  AS "50",
                    ROUND(AVG(closeprice) OVER (ORDER BY klineopentime ROWS 99 PRECEDING), {token.price_decimals})  AS "100",
                    ROUND(AVG(closeprice) OVER (ORDER BY klineopentime ROWS 199 PRECEDING), {token.price_decimals}) AS "200",
                    ROUND(AVG(closeprice) OVER (ORDER BY klineopentime ROWS 499 PRECEDING), {token.price_decimals}) AS "500"
                FROM token_historic th
                WHERE th.symbol = '{token.name}';
                ANALYZE public.token_sma;
                """
                conn.execute(text(sql))

    def __create_table_ema__(self):
        print(f"Create token_ema table ")
        with self.engine.begin() as conn:
            sql = f"""
            CREATE TABLE IF NOT EXISTS public.token_ema(
            symbol character varying(6) NOT NULL,
            klineopentime integer NOT NULL,
            "50" numeric,
            "100" numeric,
            "200" numeric,
            "500" numeric,
            CONSTRAINT "token_ema_pkey" PRIMARY KEY (symbol, klineopentime),
            CONSTRAINT symbol_klineopentime FOREIGN KEY (symbol, klineopentime)
            REFERENCES public.token_historic (symbol, klineopentime) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE NO ACTION);
            """
            conn.execute(text(sql))

    def __init_table_ema__(self, token:Token):
        print(f"Init {token.name} ema")
        if self.read_sql(f"select * from public.token_ema where symbol = '{token.name}';").shape[0] == 0:
            data = self.read_sql(f"select * from token_historic where symbol = '{token.name}' order by klineopentime;")
            column_names = self.read_sql(f"select * from public.token_ema where symbol = '{token.name}';")

            ema_df = pd.DataFrame(columns=['symbol', 'klineopentime', '50', '100', '200', '500'])
            ema_df["symbol"] = data["symbol"]
            ema_df["klineopentime"] = data["klineopentime"]


            for period in column_names.columns:
                try:
                    period = int(period)
                except ValueError:
                    continue

                multiplier = 2 / (period + 1)

                # Start med første EMA = gennemsnit af de første perioder
                ema = data.iloc[:period, 5].mean()
                ema_df.at[period-1, str(f'{period}')] = round(ema, token.price_decimals)

                # Fyld EMA direkte i kolonnen
                for i in range(period, data.shape[0]):
                    current_price = data.iloc[i, 5]
                    ema = ema + multiplier * (current_price - ema)
                    ema_df.at[i, str(f'{period}')] = round(ema, token.price_decimals)

            ema_df.dropna(inplace=True)

            try:
                ema_df.to_sql("token_ema", self.engine, if_exists="append", index=False, method="multi", chunksize=1000)
            except Exception as e:
                print(e)


    def __create_table_rsi__(self):
        print(f"Create token_rsi table ")
        with self.engine.begin() as conn:
            sql = f"""
            CREATE TABLE IF NOT EXISTS public.token_rsi(
            symbol character varying(6) NOT NULL,
            klineopentime integer NOT NULL,
            "50" NUMERIC,
            "100" NUMERIC,
            "200" NUMERIC,
            "500" NUMERIC,
            CONSTRAINT "token_rsi_pkey" PRIMARY KEY (symbol, klineopentime),
            CONSTRAINT symbol_klineopentime FOREIGN KEY (symbol, klineopentime)
            REFERENCES public.token_historic (symbol, klineopentime) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE NO ACTION);
            """
            conn.execute(text(sql))

            sql = f"""
            CREATE TABLE IF NOT EXISTS rsi_state (
            symbol character varying(6) NOT NULL,
            period INTEGER NOT NULL,
            klineopentime INTEGER NOT NULL,
            avg_gain DOUBLE PRECISION,
            avg_loss DOUBLE PRECISION,
            PRIMARY KEY (symbol, period, klineopentime)
            );
            """
            conn.execute(text(sql))

    def __init_table_rsi__(self, token: Token):
        print(f"Init {token.name} rsi")
        if self.read_sql(f"select * from public.token_rsi where symbol = '{token.name}';").shape[0] == 0:
            with self.engine.begin() as conn:
                sql = f"""
                DO $$
                DECLARE
                    rec RECORD;
                    periods INTEGER[] := ARRAY[50,100,200,500];
                    p INTEGER;
                    row_index INTEGER;
                    gains NUMERIC;
                    losses NUMERIC;
                    avg_gain NUMERIC;
                    avg_loss NUMERIC;
                    profit NUMERIC;
                    close_prev NUMERIC;
                    close_curr NUMERIC;
                    rsi NUMERIC;
                    rsi_results NUMERIC[];
                    temp_rsi RECORD;
                BEGIN
                    -- Midlertidig tabel til hurtig bulk-insert
                    CREATE TEMP TABLE temp_rsi_insert (
                        symbol character varying(6),
                        klineopentime INTEGER,
                        period INTEGER,
                        rsi_value NUMERIC
                    ) ON COMMIT DROP;
    
                    -- Beregn RSI for hver periode
                    FOREACH p IN ARRAY periods LOOP
                        RAISE NOTICE 'Beregner RSI for periode %', p;
    
                        rsi_results := ARRAY[]::DOUBLE PRECISION[];
                        gains := 0;
                        losses := 0;
                        row_index := 0;
                        close_prev := NULL;
    
                        FOR rec IN
                            SELECT klineopentime, closeprice
                            FROM token_historic th
                            WHERE th.symbol = '{token.name}'
                            ORDER BY klineopentime
                        LOOP
                            row_index := row_index + 1;
                            close_curr := rec.closeprice;
    
                            IF close_prev IS NULL THEN
                                close_prev := close_curr;
                                CONTINUE;
                            END IF;
    
                            profit := close_curr - close_prev;
    
                            IF row_index <= p+1 THEN
                                IF profit > 0 THEN
                                    gains := gains + profit;
                                ELSE
                                    losses := losses + ABS(profit);
                                END IF;
    
                                IF row_index = p+1 THEN
                                    avg_gain := gains / p;
                                    avg_loss := losses / p;
                                    rsi := 100 - (100 / (1 + avg_gain/avg_loss));
                                    rsi_results := array_append(rsi_results, rsi);
                                END IF;
                            ELSE
                                IF profit > 0 THEN
                                    avg_gain := (avg_gain*(p-1) + profit) / p;
                                    avg_loss := (avg_loss*(p-1)) / p;
                                ELSE
                                    avg_gain := (avg_gain*(p-1)) / p;
                                    avg_loss := (avg_loss*(p-1) + ABS(profit)) / p;
                                END IF;
    
                                INSERT INTO rsi_state (symbol, period, klineopentime, avg_gain, avg_loss)
                                VALUES ('{token.name}', p, rec.klineopentime, avg_gain, avg_loss) ON CONFLICT DO NOTHING;
    
                                IF avg_loss = 0 THEN
                                    rsi := 100;
                                ELSE
                                    rsi := 100 - (100 / (1 + avg_gain/avg_loss));
                                END IF;
    
                                rsi_results := array_append(rsi_results, rsi);
                            END IF;
    
                            close_prev := close_curr;
                        END LOOP;
    
                        -- Fyld temp-tabel
                        row_index := 1;
                        FOR rec IN
                            SELECT klineopentime
                            FROM token_historic th
                            WHERE th.symbol = '{token.name}'
                            ORDER BY klineopentime
                            OFFSET p
                        LOOP
                            INSERT INTO temp_rsi_insert (symbol, klineopentime, period, rsi_value)
                            VALUES ('{token.name}', rec.klineopentime, p, ROUND(rsi_results[row_index], {token.price_decimals}));
                            row_index := row_index + 1;
                        END LOOP;
                    END LOOP;
    
                    -- Bulk insert i stedet for mange UPDATEs
                    INSERT INTO token_rsi (symbol, klineopentime, "50", "100", "200", "500")
                    SELECT
                        symbol,
                        klineopentime,
                        MAX(CASE WHEN period = 50  THEN rsi_value END),
                        MAX(CASE WHEN period = 100 THEN rsi_value END),
                        MAX(CASE WHEN period = 200 THEN rsi_value END),
                        MAX(CASE WHEN period = 500 THEN rsi_value END)
                    FROM temp_rsi_insert
                    GROUP BY symbol, klineopentime;
                END $$;
                ANALYZE public.token_rsi;
                """
                conn.execute(text(sql))

    def __create_table_vwap__(self):
        print(f"Create token_vwap table ")
        with self.engine.begin() as conn:
            sql = f"""
            CREATE TABLE IF NOT EXISTS public.token_vwap(
            symbol character varying(6) NOT NULL,
            klineopentime integer NOT NULL,
            "50" numeric,
            "100" numeric,
            "200" numeric,
            "500" numeric,
            CONSTRAINT "token_vwap_pkey" PRIMARY KEY (symbol, klineopentime),
            CONSTRAINT symbol_klineopentime FOREIGN KEY (symbol, klineopentime)
            REFERENCES public.token_historic (symbol, klineopentime) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE NO ACTION);
            """
            conn.execute(text(sql))

    def __init_table_vwap__(self, token:Token):
        print(f"Init {token.name} vwap")
        if self.read_sql(f"select * from public.token_vwap where symbol = '{token.name}';").shape[0] == 0:
            with self.engine.begin() as conn:
                sql = f"""
                INSERT INTO token_vwap (symbol, klineopentime, "50", "100", "200", "500")
                SELECT
                    '{token.name}' AS symbol,
                    t.klineopentime,
                
                    ROUND(
                        SUM(((t.highprice + t.lowprice + t.closeprice)/3) * t.volume)
                            OVER (ORDER BY t.klineopentime ROWS 49 PRECEDING)
                        / NULLIF(SUM(t.volume) OVER (ORDER BY t.klineopentime ROWS 49 PRECEDING), 0), {token.price_decimals}
                    ) AS vwap50,
                
                    ROUND(
                        SUM(((t.highprice + t.lowprice + t.closeprice)/3) * t.volume)
                            OVER (ORDER BY t.klineopentime ROWS 99 PRECEDING)
                        / NULLIF(SUM(t.volume) OVER (ORDER BY t.klineopentime ROWS 99 PRECEDING), 0), {token.price_decimals}
                    ) AS vwap100,
                
                    ROUND(
                        SUM(((t.highprice + t.lowprice + t.closeprice)/3) * t.volume)
                            OVER (ORDER BY t.klineopentime ROWS 199 PRECEDING)
                        / NULLIF(SUM(t.volume) OVER (ORDER BY t.klineopentime ROWS 199 PRECEDING), 0), {token.price_decimals}
                    ) AS vwap200,
                
                    ROUND(
                        SUM(((t.highprice + t.lowprice + t.closeprice)/3) * t.volume)
                            OVER (ORDER BY t.klineopentime ROWS 499 PRECEDING)
                        / NULLIF(SUM(t.volume) OVER (ORDER BY t.klineopentime ROWS 499 PRECEDING), 0), {token.price_decimals}
                    ) AS vwap500
                
                FROM token_historic t
                WHERE t.symbol = '{token.name}'
                ON CONFLICT (symbol, klineopentime)
                DO UPDATE SET
                    "50"  = EXCLUDED."50",
                    "100" = EXCLUDED."100",
                    "200" = EXCLUDED."200",
                    "500" = EXCLUDED."500";
                ANALYZE public.token_sma;
                """
                conn.execute(text(sql))

    def __create_table_trix(self):
        print(f"Create trix table ")
        with self.engine.begin() as conn:
            sql = f"""
                    CREATE TABLE IF NOT EXISTS public.trix(
                    symbol character varying(6) NOT NULL,
                    klineopentime integer NOT NULL,
                    "50" numeric,
                    "100" numeric,
                    "200" numeric,
                    "500" numeric,
                    CONSTRAINT "trix_pkey" PRIMARY KEY (symbol, klineopentime),
                    CONSTRAINT symbol_klineopentime FOREIGN KEY (symbol, klineopentime)
                    REFERENCES public.token_historic (symbol, klineopentime) MATCH SIMPLE
                    ON UPDATE NO ACTION
                    ON DELETE NO ACTION);
                    """
            conn.execute(text(sql))



    def __init_table_with_data__(self, api:BinanceAPI, token:Token):
        print("Load data into table", token.name)
        with self.engine.begin() as conn:
            db_data = self.read_sql(f"select klineopentime from token_historic where symbol = '{token.name}' order by klineopentime desc limit 1")
            if db_data is not None:
                #init the database
                ##Check if data is stored in cold storage
                cold_storage_data = self.read_sql(f"select * from token_historic where symbol = '{token.name}' order by klineopentime limit 1")
                if not cold_storage_data.empty:
                    print("Found data in cold storage! Update that bastard")
                    while True:
                        print("Updating...")
                        if self.update_table(api=api,token=token, table_name="token_historic") is None:
                            print("No more klines to update")
                            break


                else:
                    print("Load the data from Binance")
                    tmp_start_time = api.get_candlesticks(symbol=f"{token.name}", interval=api.KlinesInterval(self.time_interval), start_time=0, limit=1).data()[0][0] // 1000
                    current_time = int(time.time())
                    while current_time > tmp_start_time:
                        end_time = tmp_start_time + self.convert_countdown_to_seconds()*1000
                        sticks = api.get_candlesticks(symbol=f"{token.name}", interval=api.KlinesInterval(self.time_interval), start_time=tmp_start_time, end_time=end_time)

                        values_sql = ", ".join(
                            f"('{token.name}', {str(stick[0])[:-3]}, {truncate_decimals(stick[1], token.price_decimals)}, {truncate_decimals(stick[2], token.price_decimals)}, {truncate_decimals(stick[3], token.price_decimals)}, {truncate_decimals(stick[4], token.price_decimals)},"
                            f"{truncate_decimals(stick[5],2)}, {truncate_decimals(stick[7],5)}, {stick[8]}, {truncate_decimals(stick[9],1)}, {truncate_decimals(stick[10],5)})"
                            for stick in sticks.data()
                        )

                        if values_sql == "":
                            tmp_start_time += self.convert_countdown_to_seconds() * 1000
                            continue

                        sql = f"""
                        INSERT INTO public.token_historic
                        (symbol, klineopentime, openprice, highprice, lowprice, closeprice, volume, quoteassetvolume, numberoftrades, takerbuybaseassetvolume, takerbuyquoteassetvolume)
                        VALUES {values_sql}
                        ON CONFLICT (symbol, klineopentime) DO UPDATE SET
                        openprice = EXCLUDED.openprice,
                        highprice = EXCLUDED.highprice,
                        lowprice = EXCLUDED.lowprice,
                        closeprice = EXCLUDED.closeprice,
                        volume = EXCLUDED.volume,
                        quoteassetvolume = EXCLUDED.quoteassetvolume,
                        numberoftrades = EXCLUDED.numberoftrades,
                        takerbuybaseassetvolume = EXCLUDED.takerbuybaseassetvolume,
                        takerbuyquoteassetvolume = EXCLUDED.takerbuyquoteassetvolume;
                        """
                        conn.execute(text(sql))
                        tmp_start_time += self.convert_countdown_to_seconds()*1000

                    conn.execute(text(f"DELETE FROM token_sma WHERE klineopentime = (SELECT klineopentime FROM token_historic ORDER BY klineopentime desc LIMIT 1) AND symbol = '{token.name}';"))
                    conn.execute(text(f"DELETE FROM token_ema WHERE klineopentime = (SELECT klineopentime FROM token_historic ORDER BY klineopentime desc LIMIT 1) AND symbol = '{token.name}';"))
                    conn.execute(text(f"DELETE FROM token_vwap WHERE klineopentime = (SELECT klineopentime FROM token_historic ORDER BY klineopentime desc LIMIT 1) AND symbol = '{token.name}';"))
                    conn.execute(text(f"DELETE FROM token_rsi WHERE klineopentime = (SELECT klineopentime FROM token_historic ORDER BY klineopentime desc LIMIT 1) AND symbol = '{token.name}';"))
                    conn.execute(text(f"DELETE FROM token_historic WHERE klineopentime = (SELECT klineopentime FROM token_historic ORDER BY klineopentime desc LIMIT 1) AND symbol = '{token.name}';"))


            else :
                print(f"token is already initialized")
            print(f"token is initialized")

    def convert_countdown_to_seconds(self):
        count_down = self.time_interval.lower()
        mapping = {
            "1d": 60 * 60 * 24,
            "12h": 60 * 60 * 12,
            "8h": 60 * 60 * 8,
            "6h": 60 * 60 * 6,
            "4h": 60 * 60 * 4,
            "2h": 60 * 60 * 2,
            "1h": 60 * 60 * 1,
            "30m": 60 * 30,
            "15m": 60 * 15,
            "5m": 60 * 5,
            "3m": 60 * 3,
            "1m": 60 * 1,
        }
        if count_down in mapping:
            return mapping[count_down]
        else:
            print("Wrong input - timer set to 1 day")
            return 86400

