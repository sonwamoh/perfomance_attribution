import datetime
from attribution import portfolio as pf
import pandas as pd
import pytest


data = [
    {
        "adj_close": 662.6,
        "date": "2023-06-01",
        "symbol": "AUROPHARMA.BSE",
        "shares": 150,
        "value": 99390.0,
    },
    {
        "adj_close": 662.35,
        "date": "2023-06-02",
        "symbol": "AUROPHARMA.BSE",
        "shares": 150,
        "value": 99352.5,
    },
    {
        "adj_close": 658.65,
        "date": "2023-06-05",
        "symbol": "AUROPHARMA.BSE",
        "shares": 150,
        "value": 98797.5,
    },
    {
        "adj_close": 663.95,
        "date": "2023-06-06",
        "symbol": "AUROPHARMA.BSE",
        "shares": 150,
        "value": 99592.5,
    },
    {
        "adj_close": 556.3,
        "date": "2023-06-01",
        "symbol": "DABUR.BSE",
        "shares": 359,
        "value": 199711.69999999998,
    },
    {
        "adj_close": 557.2,
        "date": "2023-06-02",
        "symbol": "DABUR.BSE",
        "shares": 359,
        "value": 200034.80000000002,
    },
    {
        "adj_close": 552.25,
        "date": "2023-06-05",
        "symbol": "DABUR.BSE",
        "shares": 359,
        "value": 198257.75,
    },
    {
        "adj_close": 547.2,
        "date": "2023-06-06",
        "symbol": "DABUR.BSE",
        "shares": 359,
        "value": 196444.80000000002,
    },
    {
        "adj_close": 1053.35,
        "date": "2023-06-01",
        "symbol": "GODREJCP.BSE",
        "shares": 94,
        "value": 99014.9,
    },
    {
        "adj_close": 1059.35,
        "date": "2023-06-02",
        "symbol": "GODREJCP.BSE",
        "shares": 94,
        "value": 99578.9,
    },
    {
        "adj_close": 1052.75,
        "date": "2023-06-05",
        "symbol": "GODREJCP.BSE",
        "shares": 94,
        "value": 98958.5,
    },
    {
        "adj_close": 1054.05,
        "date": "2023-06-06",
        "symbol": "GODREJCP.BSE",
        "shares": 94,
        "value": 99080.7,
    },
    {
        "adj_close": 9327.7998,
        "date": "2023-06-01",
        "symbol": "MARUTI.BSE",
        "shares": 26,
        "value": 242522.79480000003,
    },
    {
        "adj_close": 9488.7998,
        "date": "2023-06-02",
        "symbol": "MARUTI.BSE",
        "shares": 26,
        "value": 246708.79480000003,
    },
    {
        "adj_close": 9581.0996,
        "date": "2023-06-05",
        "symbol": "MARUTI.BSE",
        "shares": 26,
        "value": 249108.58959999998,
    },
    {
        "adj_close": 9731.8496,
        "date": "2023-06-06",
        "symbol": "MARUTI.BSE",
        "shares": 26,
        "value": 253028.08959999998,
    },
    {
        "adj_close": 987.5,
        "date": "2023-06-01",
        "symbol": "SUNPHARMA.BSE",
        "shares": 202,
        "value": 199475.0,
    },
    {
        "adj_close": 999.35,
        "date": "2023-06-02",
        "symbol": "SUNPHARMA.BSE",
        "shares": 202,
        "value": 201868.7,
    },
    {
        "adj_close": 1009.7,
        "date": "2023-06-05",
        "symbol": "SUNPHARMA.BSE",
        "shares": 202,
        "value": 203959.40000000002,
    },
    {
        "adj_close": 1013.55,
        "date": "2023-06-06",
        "symbol": "SUNPHARMA.BSE",
        "shares": 202,
        "value": 204737.09999999998,
    },
    {
        "adj_close": 535.25,
        "date": "2023-06-01",
        "symbol": "TATAMOTORS.BSE",
        "shares": 280,
        "value": 149870.0,
    },
    {
        "adj_close": 535.75,
        "date": "2023-06-02",
        "symbol": "TATAMOTORS.BSE",
        "shares": 280,
        "value": 150010.0,
    },
    {
        "adj_close": 546.45,
        "date": "2023-06-05",
        "symbol": "TATAMOTORS.BSE",
        "shares": 280,
        "value": 153006.0,
    },
    {
        "adj_close": 555.65,
        "date": "2023-06-06",
        "symbol": "TATAMOTORS.BSE",
        "shares": 280,
        "value": 155582.0,
    },
]

sector_returns = [
    {
        "alloc_port": 0.4,
        "date": "6/14/2023",
        "sector": "Auto",
        "returns_port": 0.12,
        "alloc_idx": 0.5,
        "returns_idx": 0.1,
    },
    {
        "alloc_port": 0.3,
        "date": "6/15/2023",
        "sector": "Pharma",
        "returns_port": 0.1,
        "alloc_idx": 0.2,
        "returns_idx": 0.12,
    },
    {
        "alloc_port": 0.3,
        "date": "6/16/2023",
        "sector": "FMCG",
        "returns_port": 0.08,
        "alloc_idx": 0.3,
        "returns_idx": 0.08,
    },
]


portfolio_attribution_res = [{"selec_val": 0.006, "alloc_val": 0.002, "interact_val": -0.004, "alpha_val": 0.004}]

build_portfolio_data = [
    {
        "adj_close": 9731.8496,
        "date": "6/6/2023",
        "symbol": "MARUTI.BSE",
        "allocation": 0.4,
        "amount": 1000000,
        "shares": 42,
        "value": 408737.6832,
    },
    {
        "adj_close": 9581.0996,
        "date": "6/5/2023",
        "symbol": "MARUTI.BSE",
        "allocation": 0.4,
        "amount": 1000000,
        "shares": 42,
        "value": 402406.1832,
    },
    {
        "adj_close": 9488.7998,
        "date": "6/2/2023",
        "symbol": "MARUTI.BSE",
        "allocation": 0.4,
        "amount": 1000000,
        "shares": 42,
        "value": 398529.5916,
    },
    {
        "adj_close": 9327.7998,
        "date": "6/1/2023",
        "symbol": "MARUTI.BSE",
        "allocation": 0.4,
        "amount": 1000000,
        "shares": 42,
        "value": 391767.5916,
    },
    {
        "adj_close": 2668.2558,
        "date": "6/6/2023",
        "symbol": "HINDUNILVR.BSE",
        "allocation": 0.5,
        "amount": 1000000,
        "shares": 186,
        "value": 496295.5788,
    },
    {
        "adj_close": 2674.5543,
        "date": "6/5/2023",
        "symbol": "HINDUNILVR.BSE",
        "allocation": 0.5,
        "amount": 1000000,
        "shares": 186,
        "value": 497467.0998,
    },
    {
        "adj_close": 2694.3427,
        "date": "6/2/2023",
        "symbol": "HINDUNILVR.BSE",
        "allocation": 0.5,
        "amount": 1000000,
        "shares": 186,
        "value": 501147.7422,
    },
    {
        "adj_close": 2675.8934,
        "date": "6/1/2023",
        "symbol": "HINDUNILVR.BSE",
        "allocation": 0.5,
        "amount": 1000000,
        "shares": 186,
        "value": 497716.1724,
    },
    {
        "adj_close": 547.2,
        "date": "6/6/2023",
        "symbol": "DABUR.BSE",
        "allocation": 0.1,
        "amount": 1000000,
        "shares": 179,
        "value": 97948.8,
    },
    {
        "adj_close": 552.25,
        "date": "6/5/2023",
        "symbol": "DABUR.BSE",
        "allocation": 0.1,
        "amount": 1000000,
        "shares": 179,
        "value": 98852.75,
    },
    {
        "adj_close": 557.2,
        "date": "6/2/2023",
        "symbol": "DABUR.BSE",
        "allocation": 0.1,
        "amount": 1000000,
        "shares": 179,
        "value": 99738.8,
    },
    {
        "adj_close": 556.3,
        "date": "6/1/2023",
        "symbol": "DABUR.BSE",
        "allocation": 0.1,
        "amount": 1000000,
        "shares": 179,
        "value": 99577.7,
    },
]

build_index_data = [
    {
        "adj_close": 9731.8496,
        "date": "6/6/2023",
        "symbol": "MARUTI.BSE",
        "allocation": 0.15,
        "amount": 1000000,
        "shares": 16,
        "value": 155709.5936,
    },
    {
        "adj_close": 9581.0996,
        "date": "6/5/2023",
        "symbol": "MARUTI.BSE",
        "allocation": 0.15,
        "amount": 1000000,
        "shares": 16,
        "value": 153297.5936,
    },
    {
        "adj_close": 9488.7998,
        "date": "6/2/2023",
        "symbol": "MARUTI.BSE",
        "allocation": 0.15,
        "amount": 1000000,
        "shares": 16,
        "value": 151820.7968,
    },
    {
        "adj_close": 9327.7998,
        "date": "6/1/2023",
        "symbol": "MARUTI.BSE",
        "allocation": 0.15,
        "amount": 1000000,
        "shares": 16,
        "value": 149244.7968,
    },
    {
        "adj_close": 2668.2558,
        "date": "6/6/2023",
        "symbol": "HINDUNILVR.BSE",
        "allocation": 0.1,
        "amount": 1000000,
        "shares": 37,
        "value": 98725.4646,
    },
    {
        "adj_close": 2674.5543,
        "date": "6/5/2023",
        "symbol": "HINDUNILVR.BSE",
        "allocation": 0.1,
        "amount": 1000000,
        "shares": 37,
        "value": 98958.5091,
    },
    {
        "adj_close": 2694.3427,
        "date": "6/2/2023",
        "symbol": "HINDUNILVR.BSE",
        "allocation": 0.1,
        "amount": 1000000,
        "shares": 37,
        "value": 99690.6799,
    },
    {
        "adj_close": 2675.8934,
        "date": "6/1/2023",
        "symbol": "HINDUNILVR.BSE",
        "allocation": 0.1,
        "amount": 1000000,
        "shares": 37,
        "value": 99008.0558,
    },
    {
        "adj_close": 547.2,
        "date": "6/6/2023",
        "symbol": "DABUR.BSE",
        "allocation": 0.2,
        "amount": 1000000,
        "shares": 359,
        "value": 196444.8,
    },
    {
        "adj_close": 552.25,
        "date": "6/5/2023",
        "symbol": "DABUR.BSE",
        "allocation": 0.2,
        "amount": 1000000,
        "shares": 359,
        "value": 198257.75,
    },
    {
        "adj_close": 557.2,
        "date": "6/2/2023",
        "symbol": "DABUR.BSE",
        "allocation": 0.2,
        "amount": 1000000,
        "shares": 359,
        "value": 200034.8,
    },
    {
        "adj_close": 556.3,
        "date": "6/1/2023",
        "symbol": "DABUR.BSE",
        "allocation": 0.2,
        "amount": 1000000,
        "shares": 359,
        "value": 199711.7,
    },
    {
        "adj_close": 3705.55,
        "date": "6/6/2023",
        "symbol": "EICHERMOT.BSE",
        "allocation": 0.2,
        "amount": 1000000,
        "shares": 53,
        "value": 196394.15,
    },
    {
        "adj_close": 3670.1001,
        "date": "6/5/2023",
        "symbol": "EICHERMOT.BSE",
        "allocation": 0.2,
        "amount": 1000000,
        "shares": 53,
        "value": 194515.3053,
    },
    {
        "adj_close": 3699.6001,
        "date": "6/2/2023",
        "symbol": "EICHERMOT.BSE",
        "allocation": 0.2,
        "amount": 1000000,
        "shares": 53,
        "value": 196078.8053,
    },
    {
        "adj_close": 3715.1001,
        "date": "6/1/2023",
        "symbol": "EICHERMOT.BSE",
        "allocation": 0.2,
        "amount": 1000000,
        "shares": 53,
        "value": 196900.3053,
    },
    {
        "adj_close": 1054.05,
        "date": "6/6/2023",
        "symbol": "GODREJCP.BSE",
        "allocation": 0.1,
        "amount": 1000000,
        "shares": 94,
        "value": 99080.7,
    },
    {
        "adj_close": 1052.75,
        "date": "6/5/2023",
        "symbol": "GODREJCP.BSE",
        "allocation": 0.1,
        "amount": 1000000,
        "shares": 94,
        "value": 98958.5,
    },
    {
        "adj_close": 1059.35,
        "date": "6/2/2023",
        "symbol": "GODREJCP.BSE",
        "allocation": 0.1,
        "amount": 1000000,
        "shares": 94,
        "value": 99578.9,
    },
    {
        "adj_close": 1053.35,
        "date": "6/1/2023",
        "symbol": "GODREJCP.BSE",
        "allocation": 0.1,
        "amount": 1000000,
        "shares": 94,
        "value": 99014.9,
    },
    {
        "adj_close": 555.65,
        "date": "6/6/2023",
        "symbol": "TATAMOTORS.BSE",
        "allocation": 0.25,
        "amount": 1000000,
        "shares": 467,
        "value": 259488.55,
    },
    {
        "adj_close": 546.45,
        "date": "6/5/2023",
        "symbol": "TATAMOTORS.BSE",
        "allocation": 0.25,
        "amount": 1000000,
        "shares": 467,
        "value": 255192.15,
    },
    {
        "adj_close": 535.75,
        "date": "6/2/2023",
        "symbol": "TATAMOTORS.BSE",
        "allocation": 0.25,
        "amount": 1000000,
        "shares": 467,
        "value": 250195.25,
    },
    {
        "adj_close": 535.25,
        "date": "6/1/2023",
        "symbol": "TATAMOTORS.BSE",
        "allocation": 0.25,
        "amount": 1000000,
        "shares": 467,
        "value": 249961.75,
    },
]

port_sector_returns_data = [
    {"sector": "AUTO", "date": "6/6/2023", "sector_alloc": 0.592477574, "sector_returns": 0.043316732},
    {"sector": "FMCG", "date": "6/6/2023", "sector_alloc": 0.407522426, "sector_returns": -0.005105516},
]

stocks_to_weight_port = [("MARUTI.BSE", 0.4), ("HINDUNILVR.BSE", 0.3), ("DABUR.BSE", 0.3)]

stocks_to_weight_idx = [
    ("MARUTI.BSE", 0.15),
    ("HINDUNILVR.BSE", 0.1),
    ("DABUR.BSE", 0.2),
    ("EICHERMOT.BSE", 0.2),
    ("GODREJCP.BSE", 0.10),
    ("TATAMOTORS.BSE", 0.25),
]
stocks_to_sector_port = [
    ("MARUTI.BSE", "AUTO"),
    ("HINDUNILVR.BSE", "FMCG"),
    ("DABUR.BSE", "FMCG"),
]
stocks_to_sector_idx = [
    ("MARUTI.BSE", "AUTO"),
    ("HINDUNILVR.BSE", "FMCG"),
    ("DABUR.BSE", "FMCG"),
    ("EICHERMOT.BSE", "AUTO"),
    ("GODREJCP.BSE", "FMCG"),
    ("TATAMOTORS.BSE", "AUTO"),
]


class TestBuildPortfolio:
    def test_build_portfolio(self):
        test_portfolio = pd.DataFrame(build_portfolio_data)
        test_portfolio = (
            test_portfolio.drop(columns=["allocation", "amount"])
            .sort_values(by=["symbol", "date"])
            .reset_index(drop=True)
        )
        port = pf.build_portfolio(
            1000000, stocks_to_weight_port, datetime.datetime(2023, 6, 1), datetime.datetime(2023, 6, 6)
        )
        assert sum(test_portfolio["value"]) == sum(port["value"])
        assert float(sum(test_portfolio["shares"])) == sum(port["shares"])

    def test_incorrect_weight(self):
        test_portfolio = pd.DataFrame(build_portfolio_data)
        test_portfolio = (
            test_portfolio.drop(columns=["allocation", "amount"])
            .sort_values(by=["symbol", "date"])
            .reset_index(drop=True)
        )
        print(test_portfolio)
        with pytest.raises(pf.MyCustomException):
            pf.build_portfolio(
                1000000, stocks_to_weight_port, datetime.datetime(2023, 6, 1), datetime.datetime(2023, 6, 6)
            )

    def test_build_index(self):
        test_index = pd.DataFrame(build_index_data)
        test_index = (
            test_index.drop(columns=["allocation", "amount"]).sort_values(by=["symbol", "date"]).reset_index(drop=True)
        )
        print(test_index)
        idx = pf.build_portfolio(
            1000000, stocks_to_weight_idx, datetime.datetime(2023, 6, 1), datetime.datetime(2023, 6, 6)
        )
        print(idx)
        assert (sum(test_index["value"]) == sum(idx["value"])) & (
            float(sum(test_index["shares"])) == sum(idx["shares"])
        )


class TestPortfolioReturns:
    def test_portfolio_returns(self):
        stocks_to_weight_port = [("MARUTI.BSE", 0.4), ("HINDUNILVR.BSE", 0.5), ("DABUR.BSE", 0.1)]

        port = pf.build_portfolio(
            1000000, stocks_to_weight_port, datetime.datetime(2023, 6, 1), datetime.datetime(2023, 6, 6)
        )
        port_sector_returns = pf.get_portfolio_returns(port, stocks_to_sector_port)
        assert round(port_sector_returns.iloc[0, 3], 4) == 0.0433
        assert round(port_sector_returns.iloc[1, 3], 4) == -0.0051


port = pf.build_portfolio(1000000, stocks_to_weight_port, datetime.datetime(2023, 6, 1), datetime.datetime(2023, 6, 6))
idx = pf.build_portfolio(1000000, stocks_to_weight_idx, datetime.datetime(2023, 6, 1), datetime.datetime(2023, 6, 6))


class TestAttribution:
    def test_attribution_selection_val(self):
        port_res = pf.perform_portfolio_attribution(port, idx, stocks_to_sector_port, stocks_to_sector_idx)
        res = pd.DataFrame(portfolio_attribution_res).reset_index(drop=True)
        print(res)
        print(round(port_res["selec_val"].at[0], 3))
        print(res["selec_val"].at[0])


#     def test_allocation_val(self):
#         df = pd.DataFrame(sector_returns).reset_index(drop=True)
#         port_res = pf.perform_portfolio_attribution(df)
#         port_res = pd.DataFrame(port_res, index=[0])
#         res = pd.DataFrame(portfolio_attribution_res).reset_index(drop=True)
#         assert round(port_res["alloc_val"].at[0], 3) == res["alloc_val"].at[0]

#     def test_interaction_val(self):
#         df = pd.DataFrame(sector_returns).reset_index(drop=True)
#         port_res = pf.perform_portfolio_attribution(df)
#         port_res = pd.DataFrame(port_res, index=[0])
#         res = pd.DataFrame(portfolio_attribution_res).reset_index(drop=True)
#         assert round(port_res["interact_val"].at[0], 3) == res["interact_val"].at[0]

#     def test_alpha_val(self):
#         df = pd.DataFrame(sector_returns).reset_index(drop=True)
#         port_res = pf.perform_portfolio_attribution(df)
#         port_res = pd.DataFrame(port_res, index=[0])
#         res = pd.DataFrame(portfolio_attribution_res).reset_index(drop=True)
#         assert round(port_res["alpha_val"].at[0], 3) == res["alpha_val"].at[0]
