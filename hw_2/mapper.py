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
    try:
        price = int(line.strip().split(",")[-7])
        print(price)
        return price
    except ValueError:
        raise ValueError
    except IndexError:
        IndexError


def compute_static_params(mean: int, vals: list) -> tuple:
    """Compute chunk size, mean and var."""
    size = len(vals)
    if size:
        mean /= size
    else:
        return 0, 0, 0
    var = sum(vals) / size - mean**2  # fast var computing
    return size, mean, var


def get_lines():
    """Generator for input line."""
    with open("AB_NYC_2019.csv", "r", encoding="utf-8") as fin:
        return fin.readlines()
        for line in stdin:
            yield line


def process_data():
    """Process input data."""
    mean, prices = 0, []
    for line in get_lines():
        try:
            price = process_line(line)
            mean += price
            prices.append(price**2)
        except ValueError:
            continue
        except IndexError:
            continue
    size, mean, var = compute_static_params(mean, prices)
    print(size, mean, var)


if __name__ == "__main__":
    process_data()
