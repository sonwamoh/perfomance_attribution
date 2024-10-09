from attribution import portfolio_oop as pf
import pandas as pd
import datetime
from datetime import datetime

stocks_to_weights_port = [
    ("MARUTI.BSE", 0.5),
    ("HINDUNILVR.BSE", 0.3),
    ("INFY.BSE", 0.2),
]
stocks_to_sectors_port = [
    ("MARUTI.BSE", "AUTO"),
    ("HINDUNILVR.BSE", "FMCG"),
    ("INFY.BSE", "IT"),
]
port = pf.Portfolio(
    10000000,
    stocks_to_weights_port,
    stocks_to_sectors_port,
    datetime(2023, 1, 1),
    datetime(2023, 1, 4),
)
port.get_portfolio_stock_returns()

stocks_to_weights_port = [
    ("MARUTI.BSE", 0.5),
    ("HINDUNILVR.BSE", 0.3),
    ("TATAMOTORS.BSE", 0.2),
]
stocks_to_sectors_port = [
    ("MARUTI.BSE", "AUTO"),
    ("HINDUNILVR.BSE", "FMCG"),
    ("TATAMOTORS.BSE", "AUTO"),
]
port1 = pf.Portfolio(
    10000000,
    stocks_to_weights_port,
    stocks_to_sectors_port,
    datetime(2023, 1, 1),
    datetime(2023, 1, 4),
)


class TestReturns:
    def test_portfolio_stock_returns(self):
        df = port.get_portfolio_stock_returns()
        assert round(sum(df["stock_returns"]), 4) == -0.0259

    def test_portfolio_sector_returns(self):
        df = port1.get_portfolio_sector_returns()
        assert round(sum(df["sector_returns"]), 4) == -0.0137


class TestContribution:
    def test_stock_wise_contribution(self):
        df = port.stock_wise_contribution()
        assert round(sum(df["contributions"]), 4) == -0.0054

    def test_sector_wise_contribution(self):
        df = port1.sector_wise_contribution()
        assert round(sum(df["contribution"]), 4) == -0.0062
