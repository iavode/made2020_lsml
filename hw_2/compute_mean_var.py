#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 28 21:15:03 2020.

@author: Odegov Ilya
"""
from pandas import read_csv


def compute_price_mean_var(price):
    """Compute price mean value and var."""
    price_mean, price_var = price.mean(skipna=True), price.var(skipna=True)
    print(f"Column price have mean value {price_mean} with var {price_var}")


def get_column(column):
    """Get column from dataset."""
    return read_csv("AB_NYC_2019.csv")[column]


if __name__ == "__main__":
    compute_price_mean_var(get_column(column="price"))
