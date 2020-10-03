#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 28 21:15:03 2020.

@author: Odegov Ilya
"""
from sys import stdin


def process_line(line) -> int:
    """Process input line."""
    # check input line type
    price = int(line.strip().split(",")[-7])
    return price


def compute_static_params(prices_sum: int, prices_squad: list) -> tuple:
    """Compute chunk size, mean and var."""
    size = len(prices_squad)
    if size:
        mean = prices_sum / size
    else:
        return 0, 0, 0
    var = sum(prices_squad) / size - mean**2  # fast var computing
    return size, mean, var


def get_line():
    """Generator for input line."""
    for line in stdin:
        yield line


def process_data():
    """Process input data."""
    prices_sum, prices_squad = 0, []
    for line in get_line():
        try:
            price = process_line(line)
            prices_sum += price
            prices_squad.append(price**2)
        except ValueError:
            continue
        except IndexError:
            continue
    # compute chunk size, mean, var
    size, mean, var = compute_static_params(prices_sum, prices_squad)
    print(size, mean, var)


def generator(data):
    for val in data:
        yield val


if __name__ == "__main__":
    process_data()
