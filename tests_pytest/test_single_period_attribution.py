import datetime
from attribution import portfolio as pf
import pandas as pd
import pytest


build_portfolio_data = [
    {"adj_close": 2537.7716, "date": "2023-01-02", "symbol": "HINDUNILVR.BSE", "shares": 118.0, "value": 299457.0488},
    {"adj_close": 2520.1654, "date": "2023-01-03", "symbol": "HINDUNILVR.BSE", "shares": 118.0, "value": 297379.5172},
    {"adj_close": 2516.2474, "date": "2023-01-04", "symbol": "HINDUNILVR.BSE", "shares": 118.0, "value": 296917.1932},
    {"adj_close": 8406.6504, "date": "2023-01-02", "symbol": "MARUTI.BSE", "shares": 83.0, "value": 697751.9832},
    {"adj_close": 8386.2998, "date": "2023-01-03", "symbol": "MARUTI.BSE", "shares": 83.0, "value": 696062.8834},
    {"adj_close": 8422.0, "date": "2023-01-04", "symbol": "MARUTI.BSE", "shares": 83.0, "value": 699026.0},
]


build_index_data = [
    {"adj_close": 2537.7716, "date": "2023-01-02", "symbol": "HINDUNILVR.BSE", "shares": 157.0, "value": 398430.1412},
    {"adj_close": 2520.1654, "date": "2023-01-03", "symbol": "HINDUNILVR.BSE", "shares": 157.0, "value": 395665.9678},
    {
        "adj_close": 2516.2474,
        "date": "2023-01-04",
        "symbol": "HINDUNILVR.BSE",
        "shares": 157.0,
        "value": 395050.84180,
    },
    {"adj_close": 8406.6504, "date": "2023-01-02", "symbol": "MARUTI.BSE", "shares": 35.0, "value": 294232.764},
    {"adj_close": 8386.2998, "date": "2023-01-03", "symbol": "MARUTI.BSE", "shares": 35.0, "value": 293520.493},
    {"adj_close": 8422.0, "date": "2023-01-04", "symbol": "MARUTI.BSE", "shares": 35.0, "value": 294770.0},
    {"adj_close": 394.8, "date": "2023-01-02", "symbol": "TATAMOTORS.BSE", "shares": 759.0, "value": 299653.2},
    {"adj_close": 394.0, "date": "2023-01-03", "symbol": "TATAMOTORS.BSE", "shares": 759.0, "value": 299046.0},
    {"adj_close": 385.75, "date": "2023-01-04", "symbol": "TATAMOTORS.BSE", "shares": 759.0, "value": 292784.25},
]


stocks_to_weights_port = [("MARUTI.BSE", 0.7), ("HINDUNILVR.BSE", 0.3)]
stocks_to_sectors_port = [("MARUTI.BSE", "AUTO"), ("HINDUNILVR.BSE", "FMCG")]

stocks_to_weights_idx = [("MARUTI.BSE", 0.3), ("HINDUNILVR.BSE", 0.4), ("TATAMOTORS.BSE", 0.3)]
stocks_to_sectors_idx = [("MARUTI.BSE", "AUTO"), ("HINDUNILVR.BSE", "FMCG"), ("TATAMOTORS.BSE", "AUTO")]


class TestBuildPortfolio:
    def test_build_portfolio(self):
        test_portfolio = pd.DataFrame(build_portfolio_data)
        test_portfolio = test_portfolio.sort_values(by=["symbol", "date"]).reset_index(drop=True)
        print(test_portfolio)
        port = pf.build_portfolio(
            1000000, stocks_to_weights_port, datetime.datetime(2023, 1, 1), datetime.datetime(2023, 1, 4)
        )
        assert sum(test_portfolio["value"]) == sum(port["value"])
        assert float(sum(test_portfolio["shares"])) == sum(port["shares"])

    def test_incorrect_weight(self):
        stocks_to_weights_port_incorrect = stocks_to_weights_port[:]
        stocks_to_weights_port_incorrect.pop()
        with pytest.raises(pf.InvalidWeightException):
            pf.build_portfolio(
                1000000, stocks_to_weights_port_incorrect, datetime.datetime(2023, 1, 1), datetime.datetime(2023, 1, 4)
            )

    def test_build_index(self):
        test_index = pd.DataFrame(build_index_data)
        test_index = test_index.sort_values(by=["symbol", "date"]).reset_index(drop=True)
        idx = pf.build_portfolio(
            1000000, stocks_to_weights_idx, datetime.datetime(2023, 1, 1), datetime.datetime(2023, 1, 4)
        )
        print(idx)
        assert (sum(test_index["value"]) == sum(idx["value"])) & (
            float(sum(test_index["shares"])) == sum(idx["shares"])
        )


port = pf.build_portfolio(
    1000000, stocks_to_weights_port, datetime.datetime(2023, 1, 1), datetime.datetime(2023, 1, 4)
)

idx = pf.build_portfolio(1000000, stocks_to_weights_idx, datetime.datetime(2023, 1, 1), datetime.datetime(2023, 1, 4))


class TestPortfolioReturns:
    def test_portfolio_returns(self):
        port_sector_returns = pf.get_portfolio_returns(port, stocks_to_sectors_port)
        assert round(port_sector_returns.iloc[0, 3], 4) == 0.0018
        assert round(port_sector_returns.iloc[1, 3], 4) == -0.0085
        assert round(sum(port_sector_returns["sector_alloc"]) == 1)

    def test_index_returns(self):
        idx_sector_returns = pf.get_portfolio_returns(idx, stocks_to_sectors_idx)
        assert round(idx_sector_returns.iloc[0, 3], 4) == -0.0107
        assert round(idx_sector_returns.iloc[1, 3], 4) == -0.0085
        assert round(sum(idx_sector_returns["sector_alloc"]) == 1)


class TestAttribution:
    def test_attribution_1(self):
        port_idx_attr = pf.perform_portfolio_attribution(port, idx, stocks_to_sectors_port, stocks_to_sectors_idx)
        assert round(port_idx_attr["alpha_val"], 4) == 0.0085

    def test_attribution_2(self):
        port_idx_attr = pf.perform_portfolio_attribution(port, idx, stocks_to_sectors_port, stocks_to_sectors_idx)
        alpha_val_1 = port_idx_attr["alpha_val"]
        port_ret = pf.get_portfolio_returns(port, stocks_to_sectors_port)
        idx_ret = pf.get_portfolio_returns(idx, stocks_to_sectors_idx)
        alpha_val_2 = sum(port_ret["sector_alloc"] * port_ret["sector_returns"]) - sum(
            idx_ret["sector_alloc"] * idx_ret["sector_returns"]
        )
        assert round(alpha_val_1, 4) == round(alpha_val_2, 4)
