#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 28 21:15:03 2020.

@author: Odegov Ilya
"""
from math import isnan
from sys import stdin


def process_line():
    """Process input line."""
    pass


def process_data():
    """Process input data."""
    mean, vals = 0, 0, []
    for line in stdin:
        val = process_line(line)
        if not isnan(val):
            mean += val()
            vals.append(val ** 2)
    size = len(vals)
    mean /= size
    var = sum(vals) / size - mean ** 2
    print(size, mean, var)


if __name__ == "__main__":
    process_data()
