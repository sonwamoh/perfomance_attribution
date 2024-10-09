from ast import Tuple
from http import client
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from attribution import portfolio as pf
from datetime import datetime
from typing import List
import requests
import pandas as pd
from fastapi.testclient import TestClient


app = FastAPI()

class PortfolioData(BaseModel):
    amount: int
    stock_to_weight: List
    start_date: str
    end_date: str


# build portfolio
@app.post("/build-portfolio/")
def build_portfolio(data: PortfolioData):
    try:
        amount = data.amount
        stock_to_weight = []
        for stk in data.stock_to_weight:
            stock_to_weight.append((stk["stock_name"], stk["stock_weight"]))
        start_date = datetime.strptime(data.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(data.end_date, "%Y-%m-%d")
        port = pf.build_portfolio(amount, stock_to_weight, start_date, end_date)
        return port.to_dict("records")
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


class RequestData(BaseModel):
    param1: List
    param2: List


"""
Month wise returns
given : series of returns
Tell me total return that I got from the series
amt: 100
2% 5% 6% 2%
give me total return 
"""
"""
102 107.1 113.526 115.796  = 15%
"""


# portfolio returns
@app.post("/portfolio-returns/")
def portfolio_returns(request_data: RequestData):
    try:
        portfolio = pd.DataFrame(request_data.param1)
        # write exception on if the param is not found
        stocks_to_sectors_list_of_lists = request_data.param2
        stocks_to_sectors = [tuple(lst) for lst in stocks_to_sectors_list_of_lists]
        portfolio_returns = pf.get_portfolio_returns(portfolio, stocks_to_sectors)
        return portfolio_returns.to_dict("records")
    # we don't see a trace path
    # we don't see the main message
    # we try to catch specific exception or custom exception
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


class PortfolioAttributionInputData(BaseModel):
    portfolio: List
    stocks_to_sectors_portfolio: List
    index: List
    stocks_to_sectors_index: List


# portfolio attribution analysis
@app.post("/perform-portfolio-attribution/")
def perform_portfolio_attribution(data: PortfolioAttributionInputData):
    try:
        portfolio = pd.DataFrame(data.portfolio)
        stocks_to_sectors_portfolio = [
            tuple(lst) for lst in data.stocks_to_sectors_portfolio
        ]
        print(stocks_to_sectors_portfolio)
        index = pd.DataFrame(data.index)
        stocks_to_sectors_index = [tuple(lst) for lst in data.stocks_to_sectors_index]
        print(stocks_to_sectors_index)
        print(portfolio, index)
        portfolio_attribution_res = pf.perform_portfolio_attribution(
            portfolio, index, stocks_to_sectors_portfolio, stocks_to_sectors_index
        )
        print(portfolio_attribution_res)
        return portfolio_attribution_res
    except Exception as e:
        print(e)
        raise HTTPException(status_code=404, detail=str(e))

"""
API Perspective
input : stock id(required) and weight(required (sum to 100 %)), start-date(required), end-date(optional) (append to id, do not override it)




"""