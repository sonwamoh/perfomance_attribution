import datetime
from attribution import portfolio as pf
import pandas as pd
import pytest


port_month1 = '{"sector":{"0":"AUTO","1":"FMCG"},"sector_alloc":{"0":0.65,"1":0.35},"sector_returns":{"0":0.09,"1":-0.03}}'
port_month2 = '{"sector":{"0":"AUTO","1":"FMCG"},"sector_alloc":{"0":0.6,"1":0.4},"sector_returns":{"0":0.02,"1":-0.01}}'
idx_month1 = '{"sector":{"0":"AUTO","1":"FMCG"},"sector_alloc":{"0":0.7,"1":0.3},"sector_returns":{"0":0.15,"1":-0.01}}'
idx_month2 = '{"sector":{"0":"AUTO","1":"FMCG"},"sector_alloc":{"0":0.68,"1":0.32},"sector_returns":{"0":0.03,"1":0.01}}'
port_month1_df = pd.read_json(port_month1)
port_month2_df = pd.read_json(port_month2)
idx_month1_df = pd.read_json(idx_month1)
idx_month2_df = pd.read_json(idx_month2)


port_multi_period = []
port_multi_period.append(port_month1_df)
port_multi_period.append(port_month2_df)
idx_multi_period = []
idx_multi_period.append(idx_month1_df)
idx_multi_period.append(idx_month2_df)


class TestReturns:
    def test_port_return(self):
        port_ret = pf.multi_period_sector_returns(port_multi_period)
        assert round(sum(port_ret['sector_returns']), 4) == 0.0721
    def test_idx_return(self):
        idx_ret = pf.multi_period_sector_returns(idx_multi_period)
        assert round(sum(idx_ret["sector_returns"]), 4) == 0.1844

class TestAttribution:
    def test_attribution_1(self):
        attr_val = pf.perform_multi_period_attribution_analysis(
            port_multi_period, idx_multi_period
        )
        alpha_val_1 = attr_val["alpha_val"]
        port_ret = pf.multi_period_sector_returns(port_multi_period)
        idx_ret = pf.multi_period_sector_returns(idx_multi_period)
        alpha_val_2 = sum(port_ret["sector_alloc"] * port_ret["sector_returns"]) - sum(
            idx_ret["sector_alloc"] * idx_ret["sector_returns"]
        )
        assert round(alpha_val_1, 4) == round(alpha_val_2, 4)
    
    def test_attribution_2(self):
        attr_val = pf.perform_multi_period_attribution_analysis(
            port_multi_period, idx_multi_period
        )
        assert round(attr_val["alpha_val"], 4) == -0.070
        
