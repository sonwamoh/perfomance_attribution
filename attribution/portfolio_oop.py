import datetime
from dotenv import load_dotenv
import pandas as pd
import requests
import os
from datetime import date, datetime, timedelta
from typing import List, Tuple
import time


class InvalidWeightException(Exception):
    pass


stock_weight_key_val = Tuple[str, float]
stock_sector_key_val = Tuple[str, str]


load_dotenv()


class Stock:
    def __init__(self, stock_id):
        self.stock_id = stock_id
        self._prices = None

    def check_for_latest_date(self):
        path = os.getenv("DB_PATH")
        files = os.listdir(path)
        symb = self.stock_id.replace(".BSE", "")
        if f"{symb}.csv" not in files:
            return False
        df = pd.read_csv(f"{path}\\{symb}.csv")
        if df is not None:
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values(by=["date"])
            lastest_date = max(df["date"])
            today = datetime.today()
            yesterday = today - timedelta(days=100)
            yesterday = pd.to_datetime(yesterday).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            if lastest_date >= yesterday:
                return True

    def fetch_from_csv(self):
        path = os.getenv("DB_PATH")
        symb = self.stock_id.replace(".BSE", "")
        if not self.check_for_latest_date():
            return None
        df = pd.read_csv(f"{path}\\{symb}.csv")
        if df is None:
            raise Exception("CSV file is blank")
        return df

    def fetch_from_api(self):
        time.sleep(15)
        url = (
            "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol="
            f"{self.stock_id}&outputsize=full&apikey=IZ1C271BYPGM2NG4"
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

        df["symbol"] = self.stock_id
        path = os.getenv("DB_PATH")
        # store the fetch stock price data in stock directory
        directory = path + "\\" + self.stock_id.replace(".BSE", "") + ".csv"
        df.to_csv(directory, index=False)
        return df

    def prices(self):
        if self._prices is None:
            self._prices = self.fetch_from_csv()
        if self._prices is None:
            self._prices = self.fetch_from_api()
        return self._prices


class Portfolio:
    def __init__(
        self,
        amount: float,
        stock_to_weight: List[stock_weight_key_val],
        stock_to_sector: List[stock_sector_key_val],
        start_date: date = None,
        end_date: date = None,
    ):
        self.amount = amount
        self.stock_to_weight = stock_to_weight
        self.stock_to_sector = stock_to_sector
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
        df_weights = pd.DataFrame(self.stock_to_weight, columns=["symbol", "weights"])
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
        stock_sector_df = pd.DataFrame(
            self.stock_to_sector, columns=["symbol", "sector"]
        )
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
        portfolio_returns = self.returns()
        index_returns = other.returns()
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
    port = Portfolio(
        10000000,
        [("MARUTI.BSE", 0.7), ("HINDUNILVR.BSE", 0.3)],
        [("MARUTI.BSE", "AUTO"), ("HINDUNILVR.BSE", "FMCG")],
        datetime(2023, 1, 1),
        datetime(2023, 1, 4),
    )

    idx = Portfolio(
        10000000,
        [("MARUTI.BSE", 0.3), ("HINDUNILVR.BSE", 0.5), ("TATAMOTORS.BSE", 0.2)],
        [
            ("MARUTI.BSE", "AUTO"),
            ("HINDUNILVR.BSE", "FMCG"),
            ("TATAMOTORS.BSE", "AUTO"),
        ],
        datetime(2023, 1, 1),
        datetime(2023, 1, 4),
    )

    """
    !Todo
    1> Implement stock and sector wise contribution [Done]
    2> Implement stock and sector wise returns [Done]
    3> Break the function for sector returns - sector allocation, sector wise returns [Done]
    4> Endpoint for alpha vantage api changed, so the columns change [Done]
    5> Assign sector while building portfolio, fetch it from the .csv file [Extended feature]
    """

    # sector wise returns
    print(port.get_portfolio_sector_returns())
    # stock wise returns
    print(port.get_portfolio_stock_returns())
    # stock wise contribution
    print(port.sector_wise_contribution())
    # sector wise contribution
    print(port.sector_wise_contribution())
