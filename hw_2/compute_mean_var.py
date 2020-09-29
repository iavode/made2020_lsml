#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 28 21:15:03 2020.

@author: Odegov Ilya
"""
from pandas import read_csv, Series


def get_column(column: str):
    """Get column from dataset."""
    colum_vals = read_csv("AB_NYC_2019.csv")[column]
    return colum_vals


def compute_mean_var(price: Series):
    """Compute price mean value and var."""
    price_mean, price_var = price.mean(skipna=True), price.var(skipna=True)
    return price_mean, price_var
#    print(f"Column price have mean value {price_mean} with var {price_var}")


def write(mean: float, var: float):
    """Create output files and write result inside."""
    with open("mean.txt", "w") as mean_fout, open("var.txt", "w") as var_fout:
        mean_fout.write(f"Column price have mean value: {mean}. ")
        mean_fout.write("Calculated by applying pandas function.")
        var_fout.write(f"Column price have mean value: {var}. ")
        var_fout.write("Calculated by applying pandas var.")


def output_prices(prices: Series):
    """Output interested columns."""
    prices = prices.values
    for val in prices[:-1]:
        print(val)
    print(prices[-1], end="")


def run():
    """Main function in script"""
    prices = get_column(column="price")
    mean, var = compute_mean_var(prices)
    write(round(mean, 9), round(var, 9))
    output_prices(prices)
    return prices


if __name__ == "__main__":
    prices = run()
