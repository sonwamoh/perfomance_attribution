import datetime
from dotenv import load_dotenv
import pandas as pd
import requests
import os
from datetime import date, datetime, timedelta
from typing import List, Tuple
import time
import sqlite3


class InvalidWeightException(Exception):
    pass


stock_weight_key_val = Tuple[str, float]
stock_sector_key_val = Tuple[str, str]


def connect_db():
    conn = sqlite3.connect("prices.db")
    create_table_prices = """
    CREATE TABLE IF NOT EXISTS prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_id INTEGER,
    date DATE,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    vol REAL,
    FOREIGN KEY (stock_id) REFERENCES stocks(id)
    );
    """
    conn.execute(create_table_prices)
    return conn


class Stock:
    def find_id_av_symbol(self):
        conn = connect_db()
        cursor = conn.cursor()
        cursor.execute(
            f"""SELECT "index" FROM stocks WHERE av_symbol='{self.av_symbol}'"""
        )
        id = int(cursor.fetchone()[0])
        conn.close()
        return id

    def __init__(self, av_symbol: str):
        self.av_symbol = av_symbol
        self.stock_id = self.find_id_av_symbol()
        self._prices = None

    def check_db_for_latest_date(self):
        conn = connect_db()
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(date) FROM Prices")
        result = cursor.fetchone()
        if result is not None and isinstance(result[0], str):
            db_latest_date = date.fromisoformat(result[0])
        else:
            return None
        yesterday = date.today() - timedelta(days=2)
        return db_latest_date >= yesterday

    def fetch_from_db(self):
        query1 = f"SELECT COUNT(*) FROM prices WHERE stock_id = {self.stock_id}"
        conn = connect_db()
        cursor = conn.cursor()
        cursor.execute(query1)
        res = int(cursor.fetchone()[0])
        query2 = f"SELECT * FROM prices WHERE stock_id = {self.stock_id}"
        if not self.check_db_for_latest_date() or res == 0:
            return None
        df = pd.read_sql_query(query2, conn)
        df = df.rename(columns={"stock_id": "symbol"})
        self._prices = df
        return self._prices

    """
    1. No data : fetch from api as full
    2. Full updated data : cache from db
    3. Partial data: call the api without outputsize= full -> check the last date in db ->  filter out the date from api 
    """

    def fetch_from_api(self):
        api_key = os.getenv("API_KEY")
        url = (
            "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol="
            f"{self.av_symbol}&outputsize=full&apikey={api_key}"
        )
        payload = {}
        headers = {}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code != 200:
            return False
        data = response.json()
        if "Time Series (Daily)" not in data:
            print(response.text, response.status_code)
            return None

        prices = []
        for k, v in data["Time Series (Daily)"].items():
            v.update({"date": k})
            prices.append(v)

        colnames = {
            "1. open": "open",
            "2. high": "high",
            "3. low": "low",
            "4. close": "close",
            "5. volume": "vol",
        }
        df = pd.DataFrame(prices).rename(colnames, axis=1)
        conn = connect_db()
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM Prices where stock_id = {self.stock_id}")
        cursor.fetchall()
        df["stock_id"] = self.stock_id
        required_order = [
            "stock_id",
            "date",
            "open",
            "high",
            "low",
            "close",
            "vol",
        ]
        df = df[required_order]
        # look for an efficient approach to insert data in prices.db
        df.to_sql("prices", conn, if_exists="append", index=False)
        df = df.rename(columns={"stock_id": "symbol"})
        self._prices = df
        return df

    def prices(self):
        if self._prices is None:
            self._prices = self.fetch_from_db()
        if self._prices is None:
            self._prices = self.fetch_from_api()
        return self._prices


def test_fetch_prices_api():
    stk1 = Stock(1, "ITC.BSE")
    stk2 = Stock(2, "MARUTI.BSE")
    conn = connect_db()
    stk1.fetch_from_api()
    stk2.fetch_from_api()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM Prices where date='2005-01-03' and stock_id='2' ")
    res = cursor.fetchall()


def execute_custom_query():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM Prices ")
    res = cursor.fetchall()


def test_empty_prices():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("DROP TABLE Prices")
    res = cursor.fetchall()
    print(res)


class Portfolio:
    def assign_sector(self):
        conn = connect_db()
        cursor = conn.cursor()
        stock_to_sector = []
        for i in self.stock_to_weight:
            stock = i[0]
            cursor.execute(f"""SELECT "sector" FROM stocks WHERE av_symbol='{stock}'""")
            sector = str(cursor.fetchone()[0])
            stock_to_sector.append((stock, sector))
        conn.close()
        return stock_to_sector

    def stock_name_based_on_id(self, stock_id: int):
        conn = connect_db()
        cursor = conn.cursor()
        cursor.execute(f"""SELECT "name" FROM stocks WHERE "index"='{stock_id}'""")
        stock_name = str(cursor.fetchone()[0])
        return stock_name

    def __init__(
        self,
        amount: float,
        stock_to_weight: List[stock_weight_key_val],
        start_date: date = None,
        end_date: date = None,
    ):
        self.amount = amount
        self.stock_to_weight = stock_to_weight
        self.stock_to_sector = self.assign_sector()
        self.start_date = start_date
        self.end_date = end_date
        self._portfolio = None
        self._returns = None
        self._attribution = None

    def portfolio(self) -> pd.DataFrame:
        weight_sum = sum(float(weight) for stock, weight in self.stock_to_weight)

        # improvised the exception raised
        tolerance = 1e-6

        if abs(weight_sum - 1.0) > tolerance:
            raise InvalidWeightException(
                f"The sum of all the weights does not allocate to 100% (1.0), as it's allocating to {weight_sum:0.2%}."
            )

        # retrieve the stcok prices from either local storage or API call
        stock_instances = [
            Stock(stock_symbol) for stock_symbol, stock_weight in self.stock_to_weight
        ]
        portfolio = [stock.prices() for stock in stock_instances]

        portfolio = pd.concat(portfolio).drop(["open", "high", "low", "vol"], axis=1)

        # filter dates of portfolio by start and end date
        portfolio["date"] = pd.to_datetime(portfolio["date"])
        portfolio = (
            portfolio[
                (portfolio["date"] >= self.start_date)
                & (portfolio["date"] <= self.end_date)
            ]
            .sort_values(by=["symbol", "date"])
            .reset_index(drop=True)
        )

        first_date_df = portfolio.groupby("symbol").first().reset_index()
        id_to_weight = []
        conn = connect_db()
        cursor = conn.cursor()
        for i in self.stock_to_weight:
            stock = i[0]
            cursor.execute(f"""SELECT "index" FROM stocks WHERE av_symbol='{stock}'""")
            index = int(cursor.fetchone()[0])
            id_to_weight.append((index, i[1]))
        df_weights = pd.DataFrame(id_to_weight, columns=["symbol", "weights"])
        df_weights["symbol"] = df_weights["symbol"].astype("int64")
        first_date_df = first_date_df.merge(df_weights)
        first_date_df["shares"] = (
            first_date_df["weights"] * self.amount
        ) // first_date_df["close"]
        portfolio = pd.merge(
            portfolio,
            first_date_df[["date", "symbol", "shares"]],
            on=["symbol", "date"],
            how="outer",
        ).sort_values(by=["symbol", "date"])
        portfolio = portfolio.fillna(method="ffill")
        portfolio["value"] = portfolio["shares"] * portfolio["close"]
        portfolio["stock_name"] = portfolio["symbol"].map(self.stock_name_based_on_id)
        self._portfolio = portfolio
        return self._portfolio

    def stock_allocation(self) -> pd.DataFrame:
        # compute total allocation value of all stocks based on dates
        portfolio = self.portfolio()
        total_val = portfolio.groupby("date")["value"].sum()
        portfolio = pd.merge(total_val, portfolio, on="date")
        new_column_names = {"value_x": "tot_alloc_val", "value_y": "value"}
        portfolio = portfolio.rename(columns=new_column_names)
        portfolio = portfolio.sort_values(by=["symbol", "date"]).drop(
            columns=["close", "shares"]
        )
        # compute stockwise allocation
        sector_alloc_val = (
            portfolio.groupby(["symbol", "date"])["value"].sum().reset_index()
        )
        portfolio = pd.merge(
            sector_alloc_val, portfolio, on=["symbol", "date"], how="inner"
        )
        portfolio = portfolio.drop(columns="value_y")
        new_column_names = {"value_x": "stock_val"}
        portfolio = portfolio.rename(columns=new_column_names)
        portfolio["stock_alloc"] = portfolio["stock_val"] / portfolio["tot_alloc_val"]
        portfolio = portfolio.drop(columns=["tot_alloc_val"]).drop_duplicates()

        return portfolio

    def get_portfolio_stock_returns(self) -> pd.DataFrame:
        portfolio = self.stock_allocation()
        first_date, last_date = min(portfolio["date"]), max(portfolio["date"])
        portfolio = portfolio[portfolio["date"].isin([first_date, last_date])]
        portfolio["stock_returns"] = portfolio.groupby("symbol")[
            "stock_val"
        ].pct_change()
        portfolio = (
            portfolio[portfolio["date"].isin([last_date])]
            .drop(columns=["stock_val"])
            .reset_index(drop=True)
        )
        return portfolio

    def sector_allocation(self) -> pd.DataFrame:
        # compute total allocation value of all stocks based on dates
        id_to_sector = []
        conn = connect_db()
        cursor = conn.cursor()
        for i in self.stock_to_sector:
            stock = i[0]
            cursor.execute(f"""SELECT "index" FROM stocks WHERE av_symbol='{stock}'""")
            index = int(cursor.fetchone()[0])
            id_to_sector.append((index, i[1]))
        stock_sector_df = pd.DataFrame(id_to_sector, columns=["symbol", "sector"])
        stock_sector_df["symbol"] = stock_sector_df["symbol"].astype("int64")
        portfolio_with_sector = pd.merge(
            stock_sector_df, self.portfolio(), on="symbol", how="inner"
        )
        total_val = portfolio_with_sector.groupby("date")["value"].sum()
        portfolio = pd.merge(total_val, portfolio_with_sector, on="date")
        new_column_names = {"value_x": "tot_alloc_val", "value_y": "value"}
        portfolio = portfolio.rename(columns=new_column_names)
        portfolio = portfolio.sort_values(by=["symbol", "date"]).drop(
            columns=["close", "shares"]
        )
        # compute sectorwise allocation
        sector_alloc_val = (
            portfolio.groupby(["sector", "date"])["value"].sum().reset_index()
        )
        portfolio = pd.merge(
            sector_alloc_val, portfolio, on=["sector", "date"], how="inner"
        )
        portfolio = portfolio.drop(columns="value_y")
        new_column_names = {"value_x": "sector_val"}
        portfolio = portfolio.rename(columns=new_column_names)
        portfolio["sector_alloc"] = portfolio["sector_val"] / portfolio["tot_alloc_val"]
        portfolio = portfolio.drop(
            columns=["symbol", "tot_alloc_val"]
        ).drop_duplicates()
        return portfolio

    def get_portfolio_sector_returns(self) -> pd.DataFrame:
        # compute sectorwise allocation
        portfolio = self.sector_allocation()
        # compute sectorwise returns based on last day
        first_date, last_date = min(portfolio["date"]), max(portfolio["date"])
        portfolio = portfolio[
            portfolio["date"].isin([first_date, last_date])
        ].sort_values(by=["sector", "date"])
        portfolio["sector_returns"] = portfolio.groupby("sector")[
            "sector_val"
        ].pct_change()
        portfolio = (
            portfolio[portfolio["date"].isin([last_date])]
            .drop(columns=["sector_val"])
            .reset_index(drop=True)
        )
        self._returns = portfolio
        return portfolio

    def stock_wise_contribution(self) -> pd.DataFrame:
        port_ret = self.get_portfolio_stock_returns()
        port_ret["contributions"] = port_ret["stock_alloc"] * port_ret["stock_returns"]
        return port_ret

    def sector_wise_contribution(self) -> pd.DataFrame:
        port_ret = self.get_portfolio_sector_returns()
        port_ret["contribution"] = port_ret["sector_alloc"] * port_ret["sector_returns"]
        return port_ret

    def attribution(self, other) -> dict:
        portfolio_returns = self.get_portfolio_sector_returns()
        index_returns = other.get_portfolio_sector_returns()
        res = pd.merge(
            portfolio_returns,
            index_returns,
            on=["date", "sector"],
            suffixes=("_port", "_idx"),
            how="outer",
        )
        res.fillna(0, inplace=True)

        # compute the effects on all the sectors
        res["selection_effect"] = (
            res["sector_returns_port"] - res["sector_returns_idx"]
        ) * res["sector_alloc_idx"]
        res["allocation_effect"] = (
            res["sector_alloc_port"] - res["sector_alloc_idx"]
        ) * res["sector_returns_idx"]
        res["interaction_effect"] = (
            res["sector_alloc_port"] - res["sector_alloc_idx"]
        ) * (res["sector_returns_port"] - res["sector_returns_idx"])

        # compute the total effect
        tot_alloc_val = sum(res["allocation_effect"])
        tot_selec_val = sum(res["selection_effect"])
        tot_interact_val = sum(res["interaction_effect"])

        # compute alpha value
        tot_alpha_val = tot_selec_val + tot_interact_val + tot_alloc_val

        # attribution result dictionary
        val = {}
        val["alloc_val"] = tot_alloc_val
        val["selec_val"] = tot_selec_val
        val["interact_val"] = tot_interact_val
        val["alpha_val"] = tot_alpha_val
        self._attribution = val
        return self._attribution


if __name__ == "__main__":
    port_2 = Portfolio(
        10000000,
        [("MARUTI.BSE", 0.4), ("ITC.BSE", 0.3), ("TITAN.BSE", 0.3)],
        datetime(2023, 1, 1),
        datetime(2023, 1, 4),
    )
    port_3 = Portfolio(
        10000000,
        [("MARUTI.BSE", 0.4), ("ITC.BSE", 0.3), ("TITAN.BSE", 0.3)],
        datetime(2023, 1, 1),
        datetime(2023, 1, 4),
    )
    print(port_3.portfolio())
