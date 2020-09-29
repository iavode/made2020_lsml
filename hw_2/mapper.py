#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 28 21:15:03 2020.

@author: Odegov Ilya
"""
from math import isnan
from sys import stdin


def process_line(line) -> int:
    """Process input line."""
    # check input line type
    if isinstance(line, str):
        # check line lenght, price can not be to long
        if len(line) < 10:
            return int(line)


def compute_static_params(mean: int, vals: list) -> tuple:
    """Compute chunk size, mean and var."""
    size = float(len(vals))
    if size:
        mean /= size
    else:
        return 0, 0, 0
    var = sum(vals) / size - mean**2  # fast var computing
    return int(size), mean, var


def get_line():
    """Generator for input line."""
    for line in stdin:
        yield line


def process_data():
    """Process input data."""
    mean, vals = 0, []
    for line in get_line():
        val = process_line(line)
        if val and not isnan(val):
            mean += val
            vals.append(val**2)
    size, mean, var = compute_static_params(mean, vals)
    print(size, mean, var)


if __name__ == "__main__":
    process_data()
