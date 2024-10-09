from attribution import total_returns as tr
import pytest


class TestMultiperiodReturns:
    def test_returns(self):
        returns = [0.02, 0.05, -0.06, -0.01, 0.08]
        total_return = tr.calculate_total_return(returns)
        assert round(total_return, 4) == 0.0764
