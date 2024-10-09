"""
total_returns = ((1+R1) * (1+R2) * (1+R3) *...* (1+RN))-1
total_returns_percentage = total_returns * 100 
"""

from ast import List
from attribution import portfolio as pf

import pandas


def calculate_total_return(returns):
    total_return = 1
    for r in returns:
        total_return *= 1 + r
    return total_return - 1
