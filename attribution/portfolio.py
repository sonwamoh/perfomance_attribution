import datetime
from dotenv import load_dotenv
import pandas as pd
import requests
import os
from datetime import date, datetime, timedelta
from typing import List, Tuple
import time
from attribution import total_returns as tr


class InvalidWeightException(Exception):
    pass


stock_weight_key_val = Tuple[str, float]
stock_sector_key_val = Tuple[str, str]


def get_prices(symbol: str) -> pd.DataFrame:
    """Fetches updated stock price data from the Alpha Vantage API if the data is not already cached in the stocks
    directory.

    Parameters:
    -----------
    symbol : str
        Stock symbol for which the data is requested.

    Returns:
    ----------
    df : pandas.DataFrame
        Dataframe containing the latest stock prices.
    """
    load_dotenv()

    # cache the stock price data from local storage if the latest data exists in the stock directory
    path = os.getenv("DB_PATH")
    files = os.listdir(path)
    symb = symbol.replace(".BSE", "")
    if f"{symb}.csv" in files:
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
                return df
    time.sleep(15)
    # fetch latest stock data from alpha vantage api
    url = (
        "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol="
        f"{symbol}&outputsize=full&apikey=IZ1C271BYPGM2NG4"
    )
    payload = {}
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload)
    if response.status_code == 200:
        data = response.json()
        if "Time Series (Daily)" in data:
            prices = []
            for k, v in data["Time Series (Daily)"].items():
                v.update({"date": k})
                prices.append(v)

            colnames = {
                "1. open": "open",
                "2. high": "high",
                "3. low": "low",
                "4. close": "close",
                "5. adjusted close": "adj_close",
                "6. volume": "vol",
                "7. dividend amount": "dividend",
                "8. split coefficient": "factor",
                "date": "date",
            }

            df = pd.DataFrame(prices).rename(colnames, axis=1)
            df["symbol"] = symbol

            path = os.getenv("DB_PATH")

            # store the fetch stock price data in stock directory
            directory = path + "\\" + symbol.replace(".BSE", "") + ".csv"
            df.to_csv(directory, index=False)
            return df
        else:
            print(response.text, response.status_code)


def build_portfolio(
    amount: float,
    stock_to_weight: List[stock_weight_key_val],
    start_date: date = None,
    end_date: date = None,
) -> pd.DataFrame:
    """constucts a portfolio based on weights allocated to stocks

    Parameters:
    -----------
    amount : float
        Total initial amount invested in portfolio

    stocks_to_weight : List of tuples with string and float key value pair
        Weights allocated to stocks

    start_date : date
        The day on which initial amount was invested in portfolio

    Returns:
    ----------
    portfolio : pandas.DataFrame
        portfolio constructed based on amount invested on first day, weights allocated based on stock
    """
    weight_sum = 0.0
    for stock, weight in stock_to_weight:
        weight_sum += float(weight)

    # improvised the exception raised
    tolerance = 1e-6

    if abs(weight_sum - 1.0) > tolerance:
        raise InvalidWeightException(
            f"The sum of all the weights does not allocate to 100% (1.0), as it's allocating to {weight_sum:0.2%}."
        )

    # retrieve the stcok prices from either local storage or API call
    portfolio = [get_prices(i[0]) for i in stock_to_weight]

    portfolio = pd.concat(portfolio).drop(
        ["open", "high", "low", "factor", "dividend", "vol", "close"], axis=1
    )

    # filter dates of portfolio by start and end date
    portfolio["date"] = pd.to_datetime(portfolio["date"])
    portfolio = (
        portfolio[(portfolio["date"] >= start_date) & (portfolio["date"] <= end_date)]
        .reset_index(drop=True)
        .sort_values(by=["symbol", "date"])
    )

    first_date_df = portfolio.groupby("symbol").first().reset_index()
    df_weights = pd.DataFrame(stock_to_weight, columns=["symbol", "weights"])
    first_date_df = first_date_df.merge(df_weights)
    first_date_df["shares"] = (first_date_df["weights"] * amount) // first_date_df[
        "adj_close"
    ]
    portfolio = pd.merge(
        portfolio,
        first_date_df[["date", "symbol", "shares"]],
        on=["symbol", "date"],
        how="outer",
    ).sort_values(by=["symbol", "date"])
    portfolio = portfolio.fillna(method="ffill")
    portfolio["value"] = portfolio["shares"] * portfolio["adj_close"]
    return portfolio


def filter_portfolio_by_dates(
    portfolio: pd.DataFrame, start_date: date, end_date: date
) -> pd.DataFrame:
    """Filter portfolio by start_date and end_date

    Parameters:
    -----------
    portfolio: pandas.DataFrame
    start_date: datetime.datetime
    end_date: datetime.datetime

    Return:
    --------
    portfolio : pandas.DataFrame
        portfolio filtered by start and end date
    """
    return portfolio[
        (portfolio["date"] >= start_date) & (portfolio["date"] <= end_date)
    ].reset_index(drop=True)


def get_portfolio_sector_allocation(
    portfolio: pd.DataFrame, stocks_sector_map: List[stock_sector_key_val]
) -> pd.DataFrame:
    """Gives us the sector wise allocations of the stocks in the portfolio

    Parameters:
    ----------
    portfolio: pandas.DataFrame
        portfolio of stocks
    stocks_sector_map: pandas.DataFrame
        maps stock to its respective sector
    Returns:
    ---------
    sector_wise_returns : pandas.DataFrame
        data
    """
    # compute total allocation value of all stocks based on dates
    stock_sector_df = pd.DataFrame(stocks_sector_map, columns=["symbol", "sector"])
    portfolio_with_sector = pd.merge(
        stock_sector_df, portfolio, on="symbol", how="inner"
    )
    total_val = portfolio_with_sector.groupby("date")["value"].sum()
    portfolio = pd.merge(total_val, portfolio_with_sector, on="date")
    new_column_names = {"value_x": "tot_alloc_val", "value_y": "value"}
    portfolio = portfolio.rename(columns=new_column_names)
    portfolio = portfolio.sort_values(by=["symbol", "date"]).drop(
        columns=["adj_close", "shares"]
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
    portfolio = portfolio.drop(columns=["symbol", "tot_alloc_val"]).drop_duplicates()
    return portfolio


def get_portfolio_returns(
    portfolio: pd.DataFrame, stocks_sector_map: List[stock_sector_key_val]
) -> pd.DataFrame:
    """Gives us the sector wise returns and allocations of the stocks in the portfolio

    Parameters:
    ----------
    portfolio: pandas.DataFrame
        portfolio of stocks
    stocks_sector_map: pandas.DataFrame
        maps stock to its respective sector
    Returns:
    ---------
    sector_wise_returns : pandas.DataFrame
        data
    """

    # compute sectorwise allocation
    portfolio = get_portfolio_sector_allocation(portfolio, stocks_sector_map)

    # compute sectorwise returns based on last day
    first_date, last_date = min(portfolio["date"]), max(portfolio["date"])
    portfolio = portfolio[portfolio["date"].isin([first_date, last_date])].sort_values(
        by=["sector", "date"]
    )
    portfolio["sector_returns"] = portfolio.groupby("sector")["sector_val"].pct_change()
    portfolio = (
        portfolio[portfolio["date"].isin([last_date])]
        .drop(columns=["sector_val"])
        .reset_index(drop=True)
    )

    return portfolio


def get_sectorwise_attribution_effect(
    portfolio: pd.DataFrame,
    index: pd.DataFrame,
    portfolio_stocks_sector_map: List[stock_sector_key_val],
    index_stocks_sector_map: List[stock_sector_key_val],
) -> pd.DataFrame:
    """Gives us the selection, allocation and intersection effect on each date avilable in

    Parameters:
    ----------
    portfolio: pandas.DataFrame
        portfolio of stocks
    stocks_sector_map: pandas.DataFrame
        maps stock to its respective sector
    Returns:
    ---------
    sector_wise_returns : pandas.DataFrame
        data
    """

    portfolio_returns = get_portfolio_returns(portfolio, portfolio_stocks_sector_map)
    index_returns = get_portfolio_returns(index, index_stocks_sector_map)
    # change it outer joinZ
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
    res["interaction_effect"] = (res["sector_alloc_port"] - res["sector_alloc_idx"]) * (
        res["sector_returns_port"] - res["sector_returns_idx"]
    )
    return res


def perform_portfolio_attribution(
    portfolio: pd.DataFrame,
    index: pd.DataFrame,
    portfolio_stocks_sector_map: List[stock_sector_key_val],
    index_stocks_sector_map: List[stock_sector_key_val],
) -> dict:
    """This function computes the selection, allocation, intereaction effect, and alpha value of index & portfolio
    Parameters:
    ----------
    res1: pandas.DataFrame
        portfolio & index sector wise returns and allocation
    Returns:
    -----------
    res : pandas.DataFrame
        attribution data dataframe
    """
    # get portfolio attribution effect
    res = get_sectorwise_attribution_effect(
        portfolio, index, portfolio_stocks_sector_map, index_stocks_sector_map
    )
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
    return [val]


def find_sector_contribution(port_ret: pd.DataFrame) -> pd.DataFrame:
    port_ret["contribution"] = port_ret["sector_alloc"] * port_ret["sector_returns"]
    return port_ret


def get_portfolio_returns_monthly_basis(
    portfolio: pd.DataFrame, stocks_sector_map: List[stock_sector_key_val]
) -> pd.DataFrame:
    """Gives us the sector wise returns and allocations of the stocks in the portfolio
    on monthly basis

    Parameters:
    ----------
    portfolio: pandas.DataFrame
        portfolio of stocks
    stocks_sector_map: pandas.DataFrame
        maps stock to its respective sector

    Returns:
    ---------
    sector_wise_returns : pandas.DataFrame
        data
    """

    # compute sectorwise allocation
    portfolio = get_portfolio_sector_allocation(portfolio, stocks_sector_map)

    # compute sectorwise returns based on last working day of every month
    portfolio["date"] = pd.to_datetime(portfolio["date"])
    portfolio["month"] = portfolio["date"].dt.month
    portfolio["year"] = portfolio["date"].dt.year
    group_by_month_and_year = portfolio.groupby(["year", "month"])
    portfolio = portfolio.drop(columns=["month", "year"])
    last_day_of_month = group_by_month_and_year["date"].max()
    portfolio = portfolio.merge(last_day_of_month)
    portfolio = portfolio.sort_values(["sector", "date"])
    portfolio["sector_returns"] = portfolio.groupby("sector")["sector_val"].pct_change()
    portfolio = (
        portfolio.drop(columns=["sector_val"])
        .reset_index(drop=True)
        .sort_values(by=["sector", "date"])
    )

    return portfolio


def get_sectorwise_attribution_effect_monthly_basis(
    portfolio: pd.DataFrame,
    index: pd.DataFrame,
    portfolio_stocks_sector_map: List[stock_sector_key_val],
    index_stocks_sector_map: List[stock_sector_key_val],
) -> pd.DataFrame:
    """Gives us the selection, allocation and intersection effect on each date avilable in

    Parameters:
    ----------
    portfolio: pandas.DataFrame
        portfolio of stocks
    stocks_sector_map: pandas.DataFrame
        maps stock to its respective sector
    Returns:
    ---------
    sector_wise_returns : pandas.DataFrame
        data
    """

    portfolio_returns = get_portfolio_returns_monthly_basis(
        portfolio, portfolio_stocks_sector_map
    )
    index_returns = get_portfolio_returns_monthly_basis(index, index_stocks_sector_map)
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
    res["interaction_effect"] = (res["sector_alloc_port"] - res["sector_alloc_idx"]) * (
        res["sector_returns_port"] - res["sector_returns_idx"]
    )
    return res


def perform_portfolio_attribution_monthly_basis(
    portfolio: pd.DataFrame,
    index: pd.DataFrame,
    portfolio_stocks_sector_map: List[stock_sector_key_val],
    index_stocks_sector_map: List[stock_sector_key_val],
) -> dict:
    """This function computes the selection, allocation, intereaction effect, and alpha value of index & portfolio on a
    monthly basis
    Parameters:
    ----------
    res1: pandas.DataFrame
        portfolio & index sector wise returns and allocation
    Returns:
    -----------
    res : pandas.DataFrame
        attribution data dataframe
    """

    # get portfolio attribution effect
    res = get_sectorwise_attribution_effect_monthly_basis(
        portfolio, index, portfolio_stocks_sector_map, index_stocks_sector_map
    )

    # compute the total effect
    tot_alloc_val = res.groupby(["date"])["allocation_effect"].sum()
    tot_selec_val = res.groupby(["date"])["selection_effect"].sum()
    tot_interact_val = res.groupby(["date"])["interaction_effect"].sum()

    # compute alpha value
    tot_alpha_val = tot_selec_val + tot_interact_val + tot_alloc_val

    # attribution result dictionary
    val = {}
    # val["alloc_val"] = tot_alloc_val
    # val["selec_val"] = tot_selec_val
    # val["interact_val"] = tot_interact_val
    val["alpha_val"] = tot_alpha_val
    return val


def multi_period_sector_returns(port_multi_period: List[pd.DataFrame]):
    """Gives us the sector returns for multi-period portfolios which are period agnostic

    Parameters:
    ----------
    port_multi_period: list
        list of dataframes of multiperiod portfolio's

    Returns:
    ---------
    val : list of dictionary
        sectorwise returns for multiperiod portfolio's
    """
    multi_period_portfolio = pd.concat(port_multi_period, axis=1)
    first_sector_alloc_column = multi_period_portfolio.loc[
        :, multi_period_portfolio.columns.str.startswith("sector_alloc")
    ].iloc[:, 0]
    portfolio = multi_period_portfolio["sector"].drop_duplicates()
    portfolio = portfolio.loc[:, ~portfolio.columns.duplicated()]
    portfolio = pd.concat([portfolio, first_sector_alloc_column], axis=1)
    multi_period_sec_returns = multi_period_portfolio[["sector_returns"]]
    list_of_series_of_returns = [
        i for i in multi_period_sec_returns.itertuples(index=False, name=None)
    ]
    final_returns = [tr.calculate_total_return(i) for i in list_of_series_of_returns]
    sector_returns = pd.DataFrame({"sector_returns": final_returns})
    portfolio = pd.concat([portfolio, sector_returns], axis=1)
    return portfolio


def perform_multi_period_attribution_analysis(
    port_multi_period: List[pd.DataFrame], idx_multi_period: List[pd.DataFrame]
):
    """Gives us the selection, allocation and intersection effect for multi-period portfolios

    Parameters:
    ----------
    port_multi_period: list
        list of dataframes of multiperiod portfolio's
    idx_multi_period: list
        list of dataframes of multiperiod indexes's

    Returns:
    ---------
    sector_wise_returns : pandas.DataFrame
        portfolio attribution values over multiperiod portfolio
    """
    port_multi_period_ret = multi_period_sector_returns(port_multi_period)
    idx_multi_period_ret = multi_period_sector_returns(idx_multi_period)
    res = pd.merge(
        port_multi_period_ret,
        idx_multi_period_ret,
        on=["sector"],
        suffixes=("_port", "_idx"),
        how="outer",
    )
    res.fillna(0, inplace=True)
    res["selection_effect"] = (
        res["sector_returns_port"] - res["sector_returns_idx"]
    ) * res["sector_alloc_idx"]
    res["allocation_effect"] = (
        res["sector_alloc_port"] - res["sector_alloc_idx"]
    ) * res["sector_returns_idx"]
    res["interaction_effect"] = (res["sector_alloc_port"] - res["sector_alloc_idx"]) * (
        res["sector_returns_port"] - res["sector_returns_idx"]
    )
    # compute the total effect
    tot_alloc_val = sum(res["allocation_effect"])
    tot_selec_val = sum(res["selection_effect"])
    tot_interact_val = sum(res["interaction_effect"])

    # attribution result dictionary
    val = {}
    val["alloc_val"] = tot_alloc_val
    val["selec_val"] = tot_selec_val
    val["interact_val"] = tot_interact_val

    # compute alpha value
    tot_alpha_val = tot_selec_val + tot_interact_val + tot_alloc_val
    val["alpha_val"] = tot_alpha_val
    return val
