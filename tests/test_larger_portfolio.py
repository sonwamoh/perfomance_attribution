from attribution import portfolio as pf
import pandas as pd
import datetime


"""
Todo 1
!Have tests for all functions
1.Build Portfolio of 15 stocks [1st Jan 2022 - 31st Dec 2022] [Done]
2.Build Index of 30 stocks [1st Jan 2022 - 31st Dec 2022] [Done]
3.Perform Attribution Analysis [Done]
4.Get the Attribution results[Done]

Todo 2
1.Build Portfolio of 15 stocks [1st Jan 2022 - 31st Dec 2022] [Done]
2.Build Index of 30 stocks [1st Jan 2022 - 31st Dec 2022] [Done]
3.Perform attribution analysis on a monthly basis

Todo 3
1. Lookup pytest.mark.paramatrize
2. Try using the above function
"""

# Convert index .csv file into list of tuples
stock_weights_idx = pd.read_csv("stock_weights_idx.csv")
stock_sector_symbol_idx = pd.read_csv("stock_sector_symbol_idx.csv")
stock_sector_symbol_comp_name_idx = pd.merge(stock_sector_symbol_idx, stock_weights_idx)
stocks_to_weights_df_idx = stock_sector_symbol_comp_name_idx[["symbol", "weight"]]
stocks_to_sectors_df_idx = stock_sector_symbol_comp_name_idx[["symbol", "sector"]]
stocks_to_weights_idx = [tuple(row) for row in stocks_to_weights_df_idx.values]
stocks_to_sectors_idx = [tuple(row) for row in stocks_to_sectors_df_idx.values]

# Convert portfolio .csv file into list of tuples
stock_weights_port = pd.read_csv("stock_weights_port.csv")
stock_sector_symbol_port = pd.read_csv("stock_sector_symbol_port.csv")
stock_sector_symbol_comp_name_port = pd.merge(stock_sector_symbol_port, stock_weights_port)
stocks_to_weights_df_port = stock_sector_symbol_comp_name_port[["symbol", "weight"]]
stocks_to_sectors_df_port = stock_sector_symbol_comp_name_port[["symbol", "sector"]]
stocks_to_weights_port = [tuple(row) for row in stocks_to_weights_df_port.values]
stocks_to_sectors_port = [tuple(row) for row in stocks_to_sectors_df_port.values]
idx = pf.build_portfolio(
    10000000, stocks_to_weights_idx, datetime.datetime(2022, 1, 3), datetime.datetime(2022, 12, 30)
)
port = pf.build_portfolio(
    10000000, stocks_to_weights_port, datetime.datetime(2022, 1, 3), datetime.datetime(2022, 12, 30)
)


class TestBuildPortfolio:
    def test_build_portfolio(self):
        idx = pf.build_portfolio(
            10000000, stocks_to_weights_idx, datetime.datetime(2022, 1, 3), datetime.datetime(2023, 12, 30)
        )
        print(idx)

    def test_build_index(self):
        port = pf.build_portfolio(
            10000000, stocks_to_weights_port, datetime.datetime(2022, 1, 3), datetime.datetime(2023, 12, 30)
        )
        print(port)


class TestSectorWiseReturn:
    def test_sector_wise_return_idx(self):
        idx_ret = pf.get_sectorwise_returns(idx, stocks_to_sectors_idx)
        print(idx_ret)

    def test_sector_wise_return_port(self):
        port_ret = pf.get_sectorwise_returns(port, stocks_to_sectors_port)
        print(port_ret)

    def test_portfolio_returns(self):
        port_idx_ret = pf.get_portfolio_returns(port, idx, stocks_to_sectors_idx)
        print(port_idx_ret)


class TestAttribution:
    def test_attribution_anlysis(self):
        port_idx_ret = pf.get_portfolio_returns(port, idx, stocks_to_sectors_idx)
        attr = pf.perform_portfolio_attribution(port_idx_ret)
        print(attr)

    def test_attribution_anlysis_monthly_basis(self):
        port_idx_ret = pf.get_portfolio_returns_monthly_basis(port, idx, stocks_to_sectors_idx, 4)
        print(port_idx_ret)
        attr = pf.perform_portfolio_attribution(port_idx_ret)
        print(attr)


        

