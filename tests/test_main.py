from ast import Tuple
from http import client
from fastapi import FastAPI, HTTPException
import app
from attribution import portfolio as pf
from typing import List
import pandas as pd
from fastapi.testclient import TestClient
from app import main

# Test Methods
client = TestClient(app)


# test build portfolio

def test_build_portfolio():
    data = {
        "amount": 100000000,
        "stock_to_weight": [
            {"stock_name": "MARUTI.BSE", "stock_weight": 0.7},
            {"stock_name": "TCS.BSE", "stock_weight": 0.3},
        ],
        "start_date": "2023-01-01",
        "end_date": "2023-01-04",
    }
    response = client.post("/build-portfolio/", json=data)
    # check if the server runs successfully
    assert response.status_code == 200
    # check if the response returned is a json
    assert isinstance(response.json(), list)
    # check if the response obtained is accurate
    assert response.json() == [
        {
            "adj_close": 8406.6504,
            "date": "2023-01-02T00:00:00",
            "symbol": "MARUTI.BSE",
            "shares": 8326.0,
            "value": 69993771.23040001,
        },
        {
            "adj_close": 8386.2998,
            "date": "2023-01-03T00:00:00",
            "symbol": "MARUTI.BSE",
            "shares": 8326.0,
            "value": 69824332.1348,
        },
        {
            "adj_close": 8422.0,
            "date": "2023-01-04T00:00:00",
            "symbol": "MARUTI.BSE",
            "shares": 8326.0,
            "value": 70121572.0,
        },
        {
            "adj_close": 3228.9105,
            "date": "2023-01-02T00:00:00",
            "symbol": "TCS.BSE",
            "shares": 9291.0,
            "value": 29999807.4555,
        },
        {
            "adj_close": 3278.622,
            "date": "2023-01-03T00:00:00",
            "symbol": "TCS.BSE",
            "shares": 9291.0,
            "value": 30461677.002,
        },
        {
            "adj_close": 3281.7908,
            "date": "2023-01-04T00:00:00",
            "symbol": "TCS.BSE",
            "shares": 9291.0,
            "value": 30491118.322800003,
        },
    ]


def test_portfolio_returns():
    data = {
        "param1": [
            {
                "adj_close": 8406.6504,
                "date": "2023-01-02T00:00:00",
                "symbol": "MARUTI.BSE",
                "shares": 8326.0,
                "value": 69993771.23040001,
            },
            {
                "adj_close": 8386.2998,
                "date": "2023-01-03T00:00:00",
                "symbol": "MARUTI.BSE",
                "shares": 8326.0,
                "value": 69824332.1348,
            },
            {
                "adj_close": 8422.0,
                "date": "2023-01-04T00:00:00",
                "symbol": "MARUTI.BSE",
                "shares": 8326.0,
                "value": 70121572.0,
            },
            {
                "adj_close": 3228.9105,
                "date": "2023-01-02T00:00:00",
                "symbol": "TCS.BSE",
                "shares": 9291.0,
                "value": 29999807.4555,
            },
            {
                "adj_close": 3278.622,
                "date": "2023-01-03T00:00:00",
                "symbol": "TCS.BSE",
                "shares": 9291.0,
                "value": 30461677.002,
            },
            {
                "adj_close": 3281.7908,
                "date": "2023-01-04T00:00:00",
                "symbol": "TCS.BSE",
                "shares": 9291.0,
                "value": 30491118.322800003,
            },
        ],
        "param2": [["MARUTI.BSE", "AUTO"], ["TCS.BSE", "IT"]],
    }

    response = client.post("/portfolio-returns/", json=data)
    # check if the server runs successfully
    assert response.status_code == 200
    # check if the response returned is a json
    assert isinstance(response.json(), list)
    # check if the response obtained is accurate
    assert response.json() == [
        {
            "sector": "AUTO",
            "date": "2023-01-04T00:00:00",
            "sector_alloc": 0.6969456017429407,
            "sector_returns": 0.0018258877519159444,
        },
        {
            "sector": "IT",
            "date": "2023-01-04T00:00:00",
            "sector_alloc": 0.3030543982570592,
            "sector_returns": 0.016377134020902906,
        },
    ]


def test_portfolio_attribution():
    data = {
        "portfolio": [
            {
                "adj_close": 8406.6504,
                "date": "2023-01-02T00:00:00",
                "symbol": "MARUTI.BSE",
                "shares": 8326.0,
                "value": 69993771.23040001,
            },
            {
                "adj_close": 8386.2998,
                "date": "2023-01-03T00:00:00",
                "symbol": "MARUTI.BSE",
                "shares": 8326.0,
                "value": 69824332.1348,
            },
            {
                "adj_close": 8422.0,
                "date": "2023-01-04T00:00:00",
                "symbol": "MARUTI.BSE",
                "shares": 8326.0,
                "value": 70121572.0,
            },
            {
                "adj_close": 3228.9105,
                "date": "2023-01-02T00:00:00",
                "symbol": "TCS.BSE",
                "shares": 9291.0,
                "value": 29999807.4555,
            },
            {
                "adj_close": 3278.622,
                "date": "2023-01-03T00:00:00",
                "symbol": "TCS.BSE",
                "shares": 9291.0,
                "value": 30461677.002,
            },
            {
                "adj_close": 3281.7908,
                "date": "2023-01-04T00:00:00",
                "symbol": "TCS.BSE",
                "shares": 9291.0,
                "value": 30491118.322800003,
            },
        ],
        "stocks_to_sectors_portfolio": [["MARUTI.BSE", "AUTO"], ["TCS.BSE", "IT"]],
        "index": [
            {
                "adj_close": 1504.0831,
                "date": "2023-01-02T00:00:00",
                "symbol": "INFY.BSE",
                "shares": 6648.0,
                "value": 9999144.448800001,
            },
            {
                "adj_close": 1502.3563,
                "date": "2023-01-03T00:00:00",
                "symbol": "INFY.BSE",
                "shares": 6648.0,
                "value": 9987664.6824,
            },
            {
                "adj_close": 1475.073,
                "date": "2023-01-04T00:00:00",
                "symbol": "INFY.BSE",
                "shares": 6648.0,
                "value": 9806285.304000001,
            },
            {
                "adj_close": 8406.6504,
                "date": "2023-01-02T00:00:00",
                "symbol": "MARUTI.BSE",
                "shares": 8326.0,
                "value": 69993771.23040001,
            },
            {
                "adj_close": 8386.2998,
                "date": "2023-01-03T00:00:00",
                "symbol": "MARUTI.BSE",
                "shares": 8326.0,
                "value": 69824332.1348,
            },
            {
                "adj_close": 8422.0,
                "date": "2023-01-04T00:00:00",
                "symbol": "MARUTI.BSE",
                "shares": 8326.0,
                "value": 70121572.0,
            },
            {
                "adj_close": 3228.9105,
                "date": "2023-01-02T00:00:00",
                "symbol": "TCS.BSE",
                "shares": 6194.0,
                "value": 19999871.637,
            },
            {
                "adj_close": 3278.622,
                "date": "2023-01-03T00:00:00",
                "symbol": "TCS.BSE",
                "shares": 6194.0,
                "value": 20307784.667999998,
            },
            {
                "adj_close": 3281.7908,
                "date": "2023-01-04T00:00:00",
                "symbol": "TCS.BSE",
                "shares": 6194.0,
                "value": 20327412.2152,
            },
        ],
        "stocks_to_sectors_index": [
            ["MARUTI.BSE", "AUTO"],
            ["TCS.BSE", "IT"],
            ["INFY.BSE", "IT"],
        ],
    }
    response = client.post("/perform-portfolio-attribution/", json=data)
    # check if the server runs successfully
    assert response.status_code == 200
    # check if the response returned is a json
    assert isinstance(response.json(), list)
    # check if the response obtained is accurate
    assert response.json() == [
        {
            "alloc_val": 0.000006618310319679825,
            "selec_val": 0.003573054214714072,
            "interact_val": 0.000029536966470130918,
            "alpha_val": 0.003609209491503883,
        }
    ]
